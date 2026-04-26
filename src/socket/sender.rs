use core::net;
use std::{collections::VecDeque, rc::Rc, time::Duration};

use crate::{
    Reliability, Timer,
    packet::{MessageId, PacketCrateBuilder, UserMessage, build_ack_map},
    socket::ssock::SimpleSock,
    window::SlidingAckWindow,
};

/// Resend 10 times per second
const RESEND_TIMER: Duration = Duration::from_millis(100);

/// The context neccessary to poll a [SendManager]
pub struct SendContext<'a> {
    pub socket: &'a mut SimpleSock,
    pub packet_builder: &'a mut PacketCrateBuilder,
    pub recv_packet_window: &'a SlidingAckWindow,
}

/// A super simple sequence counter, that just increments and wrap arounds sequence ids
struct SequenceCounter(u32);

impl SequenceCounter {
    fn new() -> Self {
        Self(0)
    }

    /// Cycle the next value
    fn next(&mut self) -> u32 {
        let next = self.0;
        self.0 = self.0.wrapping_add(1);

        next
    }
}

#[derive(Clone)]
struct QueuedMessage {
    message: UserMessage,
    timer: Option<Timer>,
}

impl QueuedMessage {
    fn new_reliable(message: UserMessage, time: Duration) -> Self {
        Self {
            message,
            timer: Some(Timer::new(time)),
        }
    }

    fn new_unreliable(message: UserMessage) -> Self {
        Self {
            message,
            timer: None,
        }
    }

    fn tick(&mut self, dt: Duration) {
        if let Some(timer) = self.timer.as_mut() {
            timer.tick(dt);
        }
    }

    /// Is this queued message ready?
    ///
    /// Unreliable messages are always ready. Reliable however, are only ready whenever their timer hits zero
    fn is_ready(&self) -> bool {
        self.timer.map(|timer| timer.timed_out()).unwrap_or(true)
    }

    fn size(&self) -> usize {
        self.message.size()
    }

    fn message_id(&self) -> Option<MessageId> {
        self.message.message_id()
    }

    fn consume(self) -> UserMessage {
        self.message
    }

    /// Update this message's timer
    fn set_timer(&mut self, new_time: Duration) {
        if let Some(timer) = self.timer.as_mut() {
            timer.set_time(new_time);
        }
    }
}

pub struct SendManager {
    /// The connection is directed to
    to: net::SocketAddr,

    /// The amount of packets per second
    packets_per_second: usize,

    /// The packet ID counter
    packet_counter: SequenceCounter,

    /// The message ID counter
    message_counter: SequenceCounter,

    /// A queue of messages to send with their respected decrementing timers
    message_queue: VecDeque<QueuedMessage>,

    /// The sliding window for our reliable messages
    ///
    /// This tracks which messages the other side has received. This is particularly useful for knowing which messages to stop resending.
    /// Another utility - it allows us to know if a reliable message can even be sent (because we have a limited window capacity)
    send_message_window: SlidingAckWindow,
}

impl SendManager {
    pub fn new(to: net::SocketAddr, packets_per_second: usize) -> Self {
        Self {
            to,
            packets_per_second,
            packet_counter: SequenceCounter::new(),
            message_counter: SequenceCounter::new(),
            message_queue: VecDeque::new(),
            send_message_window: SlidingAckWindow::new(64),
        }
    }

    /// Add this message to the message queue.
    ///
    /// Depending on its type, it will be dispatched the next frame
    pub fn queue_msg(&mut self, payload: Vec<u8>, reliability: Reliability) {
        let payload = Rc::new(payload);

        // Based on different reliability, we're going to queue them differently
        let msg = match reliability {
            // Reliable ordered/unordered get themselves resend timers
            Reliability::Reliable | Reliability::ReliableUnordered => {
                let msg_id = self.message_counter.next();

                let message = if reliability == Reliability::Reliable {
                    UserMessage::new_reliable(msg_id, payload)
                } else {
                    UserMessage::new_reliable_unordered(msg_id, payload)
                };

                // Insert a new message that must be dispatched ASAP
                QueuedMessage::new_reliable(message, Duration::ZERO)
            }

            // Unreliable however don't get themselves anything
            Reliability::Unreliable => {
                QueuedMessage::new_unreliable(UserMessage::new_unreliable(payload))
            }
        };

        self.message_queue.push_back(msg);
    }

    /// Update messages while also collecting them into a queue at the same time
    fn update_collect_candidates(&mut self, dt: Duration, candidates: &mut VecDeque<QueuedMessage>) {
        // We're going to go from back to front
        for ind in (0..self.message_queue.len()).rev() {
            // First we're going to update it
            self.message_queue[ind].tick(dt);

            // Then clone it
            let message = self.message_queue[ind].clone();

            // If the message is ready
            if message.is_ready() {
                // Make sure to only remove unreliable messages
                if message.message_id().is_none() {
                    self.message_queue.remove(ind);
                }

                // Push into the candidate list
                candidates.push_front(message);
            }
        }

        #[cfg(feature = "stress_testing")]
        {
            use crate::socket::ssock::should_reorder_messages;

            if should_reorder_messages() {
                use rand::seq::SliceRandom;

                use crate::socket::ssock::RNG_STATE;

                let (a, b) = candidates.as_mut_slices();

                // All this ugly code to essentially simply shuffle the message queue
                RNG_STATE.with(|rng| a.shuffle(&mut *rng.borrow_mut()));
                RNG_STATE.with(|rng| b.shuffle(&mut *rng.borrow_mut()));
            }
        }
    }

    /// A separate polling method that specialises in sending messages
    fn prepare_and_send(
        &mut self,
        socket: &mut SimpleSock,
        crate_builder: &mut PacketCrateBuilder,
        recv_message_window: &SlidingAckWindow,
        dt: Duration,
    ) {
        let mut candidates = VecDeque::with_capacity(self.message_queue.len());
        self.update_collect_candidates(dt, &mut candidates);

        let mut cant_fit_stack = Vec::new();

        // How many packets can we even send?
        let mut available_packets = (
            self.packets_per_second as f32 * dt.as_secs_f32().min(1.0)
            // No matter the delta here, we're not going to send more than our PPS in a single second
        ) as usize;

        // Build our acknowledgment map for receiver's messages
        let (ack_base, ack_map) = build_ack_map(recv_message_window);

        // While we have some available message slots
        while available_packets > 0 {
            // If there are no messages nor acks to send - we'll stop right here
            if candidates.is_empty() && ack_map == 0 {
                break;
            }

            // Put our acknowledgments
            crate_builder.put_message_acknowledgments(ack_base, ack_map);

            // Get a new packet ID
            crate_builder.set_packet_id(self.packet_counter.next());

            // While the candidate list is not empty
            while !candidates.is_empty() {
                // Extract the message
                let message = candidates.pop_front().unwrap();

                // If our crate can fit the message - put it
                if crate_builder.can_fit(message.size()) {
                    // If this message is reliable - we're going to reset its timer
                    if let Some(message_id) = message.message_id() {
                        // Find it and reset its timer
                        self.message_queue
                            .iter_mut()
                            .find(|p| matches!(p.message_id(), Some(id) if id == message_id))
                            .unwrap()
                            .set_timer(RESEND_TIMER);
                    }

                    // Consume and push it
                    crate_builder.put_user_message(message.consume());
                } else {
                    // In any other case - put it in the for-later stack
                    cant_fit_stack.push(message);
                }
            }

            // Finally, our crate is ready to go. All we need to do is build and send it
            let data = crate_builder.build();
            let _ = socket.send_to(data, self.to);

            // Decrement the amount of packets we got
            available_packets -= 1;

            // Because we'll have some messages that we couldn't fit - we're going to put them back onto the candidate list
            while let Some(message) = cant_fit_stack.pop() {
                candidates.push_front(message);
            }
        }

        // If after all this we STILL have messages to send - we're going to send them next frame
        while let Some(message) = candidates.pop_back() {
            // Only push back unreliable messages (since reliable ones were simply cloned and are still in the queue)
            if message.message_id().is_none() {
                self.message_queue.push_front(message);
            }
        }
    }

    pub fn cleanup_received_messages(&mut self) {
        let window = &self.send_message_window;

        self.message_queue.retain(|msg| match msg.message_id() {
            Some(msg_id) => !window.is_old(msg_id) && !window.is_marked(msg_id),
            None => true,
        });
    }

    /// A new base was received (all messages before it were received), thus we must shift our window
    pub fn set_send_message_received_base(&mut self, new_base: MessageId) {
        // While our base is lower than the current window position
        while new_base > self.send_message_window.window_position() {
            self.send_message_window
                .mark(self.send_message_window.window_position());
        }
    }

    /// A message ID of ours was acknowledged by the receiver
    pub fn mark_sent_message_received(&mut self, msg_id: MessageId) {
        self.send_message_window.mark(msg_id);
    }

    /// Poll the send manager (this will send all the queued messages)
    pub fn poll(&mut self, ctx: SendContext, dt: Duration) {
        self.cleanup_received_messages();
        self.prepare_and_send(ctx.socket, ctx.packet_builder, ctx.recv_packet_window, dt);
    }
}
