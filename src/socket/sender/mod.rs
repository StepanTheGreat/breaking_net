use core::net;
use std::{collections::VecDeque, rc::Rc, time::Duration};

use crate::{
    MESSAGE_WINDOW_LEN, PACKET_WINDOW_LEN, Reliability, packet::{MessageId, PacketCrateBuilder, PacketScore, PacketScoreId, PacketSeqId, UserMessage, build_ack_map}, socket::{
        SocketBackend, stats::RTTMeasurements,
    }, utils::StackVec, window::{LeadingAckWindow, SlidingAckWindow},
};

mod pwindow;
mod pscore;

use pscore::PacketScoreKeeper;
use pwindow::{PacketWindow, PacketWindowEntry};

/// Timeout ceiling for messages
const MAX_RESEND_TIMEOUT: Duration = Duration::from_secs(8);

/// Initial RTT
const INIT_RTT: Duration = Duration::from_millis(100);

/// Initial RTT deviation
const INIT_DEVIATION: Duration = Duration::from_millis(25);

/// We'll start at 0 and let the protocol figure out the packet loss by itself
const INIT_PACKET_LOSS: f64 = 0.0;

/// Keep 60 RTT samples (60 seconds)
const RTT_HISTORY_LEN: usize = 60;

/// Sets the prioritization rate for newer values. RTT is highly volatile, so an alpha of 0.1 (~10 samples) is usually good enough
const RTT_ALPHA: f64 = 0.1;

/// Packet loss is a lot less volatile, so we're keeping it at between ~20 samples
const PACKET_LOSS_ALPHA: f64 = 0.05;

/// At worst network conditions, our reduction rate will be limited at 30% of our total capacity
const MAX_PACKET_REDUCTION: f64 = 0.3;

/// Keep it at minimum at 32ms
const MIN_FORCED_RELIABLE_PACKET_TIMEOUT: Duration = Duration::from_millis(32);


/// Compute an average time a packet could take to arrive
///
/// The basic idea is to take the average time it takes for a packet to make a full round trip +
/// some amount of deviation applied on top for safety.
///
/// This is mostly used in packet window's timeouts and resend timers
pub fn safe_packet_rtt(rtt: f64, deviation: f64) -> Duration {
    Duration::from_secs_f64(rtt + 4.0 * deviation)
}

/// The context neccessary to poll a [SendManager]
pub struct SendContext<'a> {
    pub socket: &'a mut dyn SocketBackend,
    pub packet_builder: &'a mut PacketCrateBuilder,
    pub recv_packet_window: &'a LeadingAckWindow,
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
        self.0 = self.0.checked_add(1).expect("Sequence ID overflow (the connection was too long). This is a socket-level limitation.");

        next
    }

    /// Get the current value
    fn current(&self) -> u32 {
        self.0
    }
}

struct PacketScoreIdCounter(PacketScoreId);

impl PacketScoreIdCounter {
    fn new() -> Self {
        Self(0)
    }

    fn next(&mut self) -> PacketScoreId {
        let next = self.0;
        self.0 = self.0.wrapping_add(1);

        next
    } 
}


#[derive(Clone)]
struct QueuedMessage {
    message: UserMessage,
    send_time: Option<Duration>,
    attempts: u32,
}

impl QueuedMessage {
    fn new_reliable(message: UserMessage, send_time: Duration) -> Self {
        Self {
            message,
            send_time: Some(send_time),
            attempts: 0,
        }
    }

    fn new_unreliable(message: UserMessage) -> Self {
        Self {
            message,
            send_time: None,
            attempts: 0,
        }
    }

    /// Is this queued message ready?
    ///
    /// Unreliable messages are always ready. Reliable however, are only ready based on the current time
    fn is_ready(&self, time: Duration) -> bool {
        self.send_time
            .map(|send_time| send_time <= time)
            .unwrap_or(true)
    }

    fn size(&self) -> usize {
        self.message.size()
    }

    fn message_id(&self) -> Option<MessageId> {
        self.message.message_id()
    }

    pub fn is_reliable(&self) -> bool {
        self.message.is_reliable()
    }

    fn consume(self) -> UserMessage {
        self.message
    }

    /// Get the current amount of attempts to send this message. Important for exponential back-off
    fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Update this message's send time
    fn set_send_time(&mut self, new_time: Duration) {
        if let Some(send_time) = self.send_time.as_mut() {
            *send_time = new_time;
        }
    }

    /// Increment the amount of times we tried to resend this message
    fn increment_attempts(&mut self) {
        self.attempts += 1;
    }
}

pub struct SendManager {
    /// The connection is directed to
    to: net::SocketAddr,

    /// The amount of packets per second
    packets_per_second: u32,

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
    sent_message_window: SlidingAckWindow,

    /// Sliding window for our packets
    ///
    /// Helps us tell which packets we sent were received by the other socket. Specifically, it lets us:
    /// - Measure how much time it took to send a packet (RTT)
    /// - Which messages associated with each packet were received (much more efficient than message ID tracking)
    sent_packet_window: PacketWindow,

    rtt: RTTMeasurements,

    /// Keep track of packet scores to know how to pace reliable packets
    packet_score: PacketScoreKeeper,

    /// A wrapping ID counter for packet scores
    packet_score_id_counter: PacketScoreIdCounter,

    /// A timer neccessary for sending (once in a while) reliable packets, even when there are no reliable messages. Important for statistics.
    last_reliable_packet: Duration,

    /// How many packets have we sent during the last poll
    packets_sent: usize,

    /// How many bytes have we sent during the last poll
    bytes_sent: usize,

    /// Time should be managed differently
    time: Duration,
}

impl SendManager {
    pub fn new(time: Duration, to: net::SocketAddr, packets_per_second: u32) -> Self {
        let rtt = RTTMeasurements::new(INIT_RTT, INIT_DEVIATION, RTT_ALPHA, RTT_HISTORY_LEN);

        Self {
            to,
            packets_per_second,
            packet_counter: SequenceCounter::new(),
            message_counter: SequenceCounter::new(),
            message_queue: VecDeque::new(),
            sent_message_window: SlidingAckWindow::new(MESSAGE_WINDOW_LEN),
            sent_packet_window: PacketWindow::new(PACKET_WINDOW_LEN),

            last_reliable_packet: Duration::from_secs_f64(rtt.rtt()),

            packet_score: PacketScoreKeeper::new(),
            packet_score_id_counter: PacketScoreIdCounter::new(),

            packets_sent: 0,
            bytes_sent: 0,
            rtt,
            time,
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
    fn collect_candidates(&mut self, candidates: &mut VecDeque<QueuedMessage>) {
        // We're going to go from back to front
        for ind in (0..self.message_queue.len()).rev() {
            // Then clone it
            let message = self.message_queue[ind].clone();

            // If the message is ready
            if message.is_ready(self.time) {
                // Make sure to only remove unreliable messages
                if message.message_id().is_none() {
                    self.message_queue.remove(ind);
                }

                // Push into the candidate list
                candidates.push_front(message);
            }
        }
    }

    /// A separate polling method that specialises in sending messages
    fn prepare_and_send(
        &mut self,
        socket: &mut dyn SocketBackend,
        crate_builder: &mut PacketCrateBuilder,
        recv_packet_window: &LeadingAckWindow,
        dt: Duration,
    ) {
        let mut candidates = VecDeque::with_capacity(self.message_queue.len());
        self.collect_candidates(&mut candidates);

        let mut cant_fit_stack = Vec::new();

        // How many packets can we even send?
        let mut available_packets = self.compute_packet_budget(dt);

        // Only one packet per frame can be ack-only. In any other case it's wasteful
        let mut available_ack_only_packet = true;

        // Build our acknowledgment map for receiver's packets
        let (ack_base, ack_map) = build_ack_map(recv_packet_window);

        let packet_score_id = self.packet_score_id_counter.next();
        let packet_score = recv_packet_window.to_packet_score() as PacketScore;

        // TODO: Move this to a separate module

        // While we have some available message slots
        while available_packets > 0 {

            // Reliable packets are those that can transport reliable messages. We have a limited set of those.
            let is_reliable_packet = self.packet_score.has_score(
                self.sent_packet_window.in_flight(), 
                dt.as_secs_f64(), 
                self.rtt()
            );

            // Check whether only reliable messages are left
            let only_reliable_left = candidates.iter()
                .find(|msg| !msg.message.is_reliable())
                .is_none();

            // If we have no messages to send, OR we only got reliable packets left and no available score,
            // we can only send ONE acknowledgment packet.
            if candidates.is_empty() || (!is_reliable_packet && only_reliable_left) {

                // If there are no available ack-only packets - stop
                if !available_ack_only_packet {
                    break;
                }
            }

            let mut packed_rel_messages: StackVec<MessageId, 4> = StackVec::new();

            // While the candidate list is not empty
            while !candidates.is_empty() {
                // Extract the message
                let message = candidates.pop_front().unwrap();

                // We must ensure that reliable messages only get sent on reliable packets
                let appropriate_msg = if message.is_reliable() { is_reliable_packet } else { true }; 

                // If our crate can fit the message - put it
                if appropriate_msg && crate_builder.can_fit(message.size()) {
                    // If this message is reliable - we're going to reset its timer
                    if let Some(message_id) = message.message_id() {
                        // Add it to the list of packed messages by this packet
                        packed_rel_messages.push(message_id);

                        // Find it and reset its timer + increment retries
                        {
                            let (rtt, dev) = (self.rtt(), self.rtt_deviation());

                            let msg = self
                                .message_queue
                                .iter_mut()
                                .find(|p| matches!(p.message_id(), Some(id) if id == message_id))
                                .unwrap();

                            let attempts = msg.attempts() as i32;

                            // The timeout increments with every attempt, maxed out at MAX_RESEND_TIMEOUT
                            // The purpose is to stop frequently "bombarding" the recipient with packets if they're "unreachable"
                            let new_timeout = safe_packet_rtt(rtt, dev)
                                .mul_f64(2.0f64.powi(attempts))
                                .min(MAX_RESEND_TIMEOUT);

                            // Set new timeout
                            msg.set_send_time(self.time + new_timeout);

                            // Make sure to increment attempts AFTER (the first attempt is always 0, so first back-off scale is 1)
                            msg.increment_attempts();
                        }
                    }

                    // Consume and push it
                    crate_builder.put_user_message(message.consume());
                } else {
                    // In any other case - put it in the for-later stack
                    cant_fit_stack.push(message);
                }
            }

            // We get a new packet ID only for reliable packets (with reliable messages)
            let mut packet_id = if is_reliable_packet && !packed_rel_messages.is_empty() { 
                Some(self.packet_counter.next())
            } else { 
                None 
            };

            // If there was no reliable packet for a while now - make one.
            if packet_id.is_none() && self.time >= self.last_reliable_packet {
                packet_id = Some(self.packet_counter.next());
            }

            // Put our acknowledgments
            crate_builder.put_packet_acknowledgments(ack_base, ack_map);

            // Set our packet scores
            crate_builder.set_packet_score(packet_score_id, packet_score);

            // If this packet is not an ack-only packet
            if let Some(packet_id) = packet_id {
                crate_builder.set_packet_id(packet_id);

                // Register this packet to our window
                self.sent_packet_window.push_sent(
                    packet_id,
                    PacketWindowEntry::new(self.time, packed_rel_messages),
                );
            }

            // Finally, our crate is ready to go. All we need to do is build and send it
            let data = crate_builder.build();
            let _ = socket.send_to(data, self.to);

            // Update our immediate statistics
            self.packets_sent += 1;
            self.bytes_sent += data.len();

            // Make sure to consume our score if this packet was reliable
            if packet_id.is_some() {
                self.packet_score.consume_score();

                // Make sure to reset our timer
                self.last_reliable_packet = self.time + Duration::from_secs_f64(self.rtt.rtt()).max(MIN_FORCED_RELIABLE_PACKET_TIMEOUT);
            }

            // Decrement the amount of packets we got (and disable ack-only packets)
            available_packets -= 1;
            available_ack_only_packet = false;

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

    /// Compute an approximatee packet budget based of the current network conditions and delta time
    fn compute_packet_budget(&self, dt: Duration) -> u32 {
        // No matter the delta here, we're not going to send more than our PPS in a single second
        let dt = dt.as_secs_f64().min(1.0);

        let base_rtt = self.base_rtt(); // Get our base RTT
        let rtt = self.rtt(); // Get our current (volatile) RTT

        // How much does the current RTT relate to the base RTT? At best it should relate as 1 (identical), at best - 2 (two times higher)
        let relation = (rtt / base_rtt).clamp(1.0, 2.0);

        // Compute the total reduction between 0 and our MAX_PACKET_REDUCTION
        let reduction = (relation - 1.0) * MAX_PACKET_REDUCTION;

        // Our resulting budget is our PPS * our reduction * delta time. Still, ensure that there's always at least one packet
        ((self.packets_per_second as f64 * (1.0 - reduction) * dt) as u32).max(1)
    }

    // Remove all messages from the message queue that were already received by the recipient
    fn cleanup_received_messages(&mut self) {
        let window = &self.sent_message_window;

        self.message_queue.retain(|msg| match msg.message_id() {
            Some(msg_id) => !window.is_old(msg_id) && !window.is_marked(msg_id),
            None => true,
        });
    }

    /// A packet ID of ours was acknowledged by the receiver.
    /// 
    /// The delta time is important, since it allows us to take more "precise" RTT measurements.
    pub fn mark_sent_packet_received(&mut self, packet_id: PacketSeqId, dt: Duration) {
        
        if packet_id >= self.packet_counter.current() {
            // A packet that we never sent was acknowledged??
            return;
        }

        // If this packet is new - let us register and process it
        if let Some(packet) = self.sent_packet_window.mark_sent(packet_id) {
            // Compute our RTT delta and update measurements
            // The delta here is `time - (timestamp + dt/2)`.
            // We're subtracting half of our delta time, because our packet could have been received during any interval between last poll and this one
            // (which is equal to delta time). By subtracting by half, we're compensating for the artifical latency from our polling.
            let dt = self.time.saturating_sub(packet.timestamp + dt.div_f32(2.0));

            self.rtt.push(dt);

            // Mark all messages within as received
            for msg in packet.messages.as_slice() {
                self.sent_message_window.mark(*msg);
            }
        }
    }

    /// Push new packet score onto this sender. This is neccessary to know how many packets we can send at a time to our receiver without
    /// overwhelming them.
    pub fn push_new_packet_score(&mut self, id: PacketScoreId, score: PacketScore) {
        self.packet_score.push_score(id, score);
    }

    pub fn reset_immediate_stats(&mut self) {
        self.sent_packet_window.reset_immediate_stats();
        self.packets_sent = 0;
        self.bytes_sent = 0;
    }

    /// Poll the send manager (this will send all the queued messages)
    pub fn poll(&mut self, ctx: SendContext, time: Duration) {
        // Compute delta (important for PPS calculation)
        let dt = time.saturating_sub(self.time);
        self.time = time;

        self.rtt.update(dt);
        self.cleanup_received_messages();
        self.prepare_and_send(ctx.socket, ctx.packet_builder, ctx.recv_packet_window, dt);
    }

    /// Get latest RTT measurements
    pub fn rtt(&self) -> f64 {
        self.rtt.rtt()
    }

    /// Get latest median RTT deviation
    pub fn rtt_deviation(&self) -> f64 {
        self.rtt.deviation()
    }

    /// Get the median RTT which is much more stable and more representable of network's health
    pub fn base_rtt(&self) -> f64 {
        self.rtt.median()
    }

    /// Get current packet loss
    pub fn packet_loss(&self) -> f64 {
        self.sent_packet_window.packet_loss()
    }

    /// How many messages are currently queued for resending
    pub fn queued_messages(&self) -> usize {
        self.message_queue.len()
    }

    /// How many packets have we sent
    pub fn packets_sent(&self) -> usize {
        self.packets_sent
    }

    /// How many bytes have we sent
    pub fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }

    /// How many packets have we lost
    pub fn packets_lost(&self) -> usize {
        self.sent_packet_window.packets_lost()
    }
}
