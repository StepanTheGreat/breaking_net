use rand::seq::SliceRandom;
use std::{
    collections::{HashMap, HashSet, VecDeque}, net, rc::Rc
};

use crate::{
    packet::{MessageId, PacketAckMap, PacketCrateBuilder, PacketSeqId, Reliability, UserMessage, build_ack_map},
    socket::{SimpleSock, channels::{Channel, ChannelStorage}},
    window::SlidingAckWindow,
};

/// Resend 15 times per second
const RESEND_TIMER: f32 = 1.0 / 15.0;

const RTT_SMOOTH_FACTOR: f32 = 0.4;
const RTT_MAX_TIME: f32 = 1.0;

const INIT_RTT: f32 = 0.0;

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
    timer: Option<f32>
}

impl QueuedMessage {
    fn new_reliable(message: UserMessage, timer: f32) -> Self {
        Self {
            message,
            timer: Some(timer)
        }
    }

    fn new_unreliable(message: UserMessage) -> Self {
        Self {
            message, 
            timer: None
        }
    }

    fn tick(&mut self, dt: f32) {
        if let Some(timer) = self.timer.as_mut() {
            *timer = (*timer - dt).max(0.0);
        }
    }

    /// Is this queued message ready?
    fn is_ready(&self) -> bool {
        self.timer.unwrap_or(0.0) == 0.0
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
    fn set_timer(&mut self, new_time: f32) {
        if let Some(timer) = self.timer.as_mut() {
            *timer = new_time;
        }
    }
}

pub struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    /// The amount of messages per second
    messages_per_second: usize,

    /// Messages to send with their respected decrementing timers
    /// A queue of messages
    message_queue: VecDeque<QueuedMessage>,

    /// The counter to obtain sequence IDs from
    seq_counter: SequenceCounter,

    channels: ChannelStorage,

    /// The sliding window for all reliable messages
    message_window: SlidingAckWindow,

    /// Sequence IDs of messages that were **sent** by us
    self_acknowledged: HashSet<MessageId>,

    /// Sequence IDs of messages that were **received** by us
    other_acknowledged: HashSet<MessageId>,

    /// The amount packets we sent
    packets_sent: u32,

    /// The amount of packets we lost
    packets_lost: u32,

    /// A map of rtt timers
    rtt_timers: HashMap<MessageId, f32>,

    /// The approximate RTT
    rtt: f32
}

impl SocketConnection {
    pub fn new(to: net::SocketAddr) -> Self {
        let messages_per_second = 100;

        Self {
            to,

            messages_per_second,
            
            message_queue: VecDeque::new(),
            seq_counter: SequenceCounter::new(),

            message_window: SlidingAckWindow::new(128),
            channels: ChannelStorage::new(),

            self_acknowledged: HashSet::new(),
            other_acknowledged: HashSet::new(),

            packets_sent: 0,
            packets_lost: 0,

            rtt_timers: HashMap::new(),
            rtt: INIT_RTT
        }
    }

    /// Queue a new message to send through this connection ASAP
    pub fn queue_message(&mut self, reliability: Reliability, payload: Vec<u8>) {
        let payload = Rc::new(payload);

        // Based on different reliability, we're going to queue them differently
        match reliability {
            // Reliable ordered/unordered get themselves resend timers
            Reliability::Reliable | Reliability::ReliableUnordered => {
                let seq_id = self.seq_counter.next();

                let message = if reliability == Reliability::Reliable {
                    UserMessage::new_reliable(seq_id, payload)
                } else {
                    UserMessage::new_reliable_unordered(seq_id, payload)
                };

                // Insert a new message that must be dispatched ASAP
                self.message_queue
                    .push_back(QueuedMessage::new_reliable(message, 0.0));
            }

            // Unreliable however don't get themselves anything
            Reliability::Unreliable => {
                // Just push a basic unreliable message
                self.message_queue
                    .push_back(QueuedMessage::new_unreliable(UserMessage::new_unreliable(payload)));
            }
        }
    }

    /// Acknowledgments have been received on this connection
    pub fn own_acknowledgments_received(&mut self, ack_base: MessageId, ack_map: PacketAckMap) {
        // No acknowledgments
        if ack_base == 0 && ack_map == 0 {
            return;
        }
        
        // Init the cursor
        let mut cursor = 1 << (PacketAckMap::BITS-1);

        // For each bit
        for bind in 0..PacketAckMap::BITS {
            if (ack_map & cursor) > 0 {

                let seq_id = ack_base + bind;

                self.self_acknowledged.insert(seq_id);
                self.mark_rtt_received(seq_id);
            } 
            
            // Move the cursor to the right
            cursor >>=  1;
        }
    }

    pub fn other_acknowledgment_received(&mut self, ack: MessageId) {
        self.other_acknowledged.insert(ack);
    }


    /// Update messages while also collecting them into a queue at the same time
    fn update_collect_candidates(&mut self, dt: f32, candidates: &mut VecDeque<QueuedMessage>) {
        // We're going to go from back to front
        for ind in (0..self.message_queue.len()).rev() {
            // First we're going to update it
            self.message_queue[ind].tick(dt);

            // Then clone it
            let message = self.message_queue[ind].clone();

            // If the message is both acknowledged and ready - remove it from the queue
            if !(matches!(message.message_id(), Some(seq_id) if !self.self_acknowledged.contains(&seq_id)) || !message.is_ready())
            {
                self.message_queue.remove(ind);
            }

            // And if ready - add to the candidate list
            if message.is_ready() {
                candidates.push_front(message);
            }
        }

        #[cfg(feature = "stress_testing")]
        {
            use crate::socket::should_reorder_messages;

            if should_reorder_messages() {
                use crate::socket::RNG_STATE;

                let (a, b) = candidates.as_mut_slices();

                // All this ugly code to essentially simply shuffle this message queue
                RNG_STATE.with(|rng| a.shuffle(&mut *rng.borrow_mut()));
                RNG_STATE.with(|rng| b.shuffle(&mut *rng.borrow_mut()));
            }
        }
    }

    /// Update RTT timers and when some of them are maxed out - remove them 
    fn update_rtt_timers(&mut self, dt: f32) {
        self.rtt_timers.retain(|_, timer| {
            *timer = (*timer + dt).min(1.0);

            // If our message timed out
            if *timer == RTT_MAX_TIME {

                // We would also like to increment the packet loss counter
                self.packets_lost += 1;

                false
            } else {
                true
            }

        });
    }

    /// Mark this sequence ID as received in the RTT calculations
    fn mark_rtt_received(&mut self, seq_id: MessageId) {
        // If it's actually present - we're going to pop it

        if let Some(time) = self.rtt_timers.remove(&seq_id) {
            // Update our rtt according to the smoothed average formula
            
            self.rtt += RTT_SMOOTH_FACTOR*(time-self.rtt);
        }
    }

    /// Add an RTT tracker 
    fn add_rtt_tracker(&mut self, ack: MessageId) {
        self.rtt_timers.insert(ack, 0.0);
    }

    /// A separate polling method that specialises in sending messages
    fn prepare_and_send(
        &mut self, 
        socket: &mut SimpleSock, 
        crate_builder: &mut PacketCrateBuilder, 
        dt: f32
    ) {
        let mut candidates = VecDeque::with_capacity(self.message_queue.len());
        self.update_collect_candidates(dt, &mut candidates);

        let mut cant_fit_stack = Vec::new();

        // How many messages can we even send?
        let mut available_messages = (
            self.messages_per_second as f32 * dt.clamp(0.0, 1.0)
            // No matter the delta here, we're not going to send more than our PPS in a single second
        ) as usize;

        // Add ONE acknowledgment ID into our table
        for message in candidates.iter() {
            if let Some(seq_id) = message.message_id() {
                if self.rtt_timers.contains_key(&seq_id) {
                    continue;
                }

                // Insert it at 0
                self.add_rtt_tracker(seq_id);
                
                self.packets_sent += 1;

                break
            }
        }

        // Build our acknowledgment map
        let (ack_base, ack_map) = {
            let mut acknowledgments: Vec<MessageId> = self.other_acknowledged.iter().copied().collect();
            acknowledgments.sort();

            build_ack_map(&acknowledgments)
        };

        // Only keep acknowledgments that didn't fit into our acknowledgment map
        self.other_acknowledged.retain(|seq_id| *seq_id > ack_base+PacketAckMap::BITS);

        // While we have some available message slots
        while available_messages > 0 {
            // If there are no messages nor acks to send - we'll stop right here
            if candidates.is_empty() && ack_map == 0 {
                break;
            }

            // Put our acknowledgments
            crate_builder.put_acknowledgments(ack_base, ack_map);

            // While the candidate list is not empty
            while !candidates.is_empty() {
                // Extract the message
                let message = candidates.pop_front().unwrap();

                // If our crate can fit our message - put it
                if crate_builder.can_fit(message.size()) {
                    // If our message is unacknowledged - we're going to reset its timer
                    if let Some(seq_id) = message.message_id() {
                        if self.self_acknowledged.contains(&seq_id) {
                            continue;
                        }

                        // Find it and reset its timer
                        self.message_queue
                            
                            .iter_mut()
                            .find(|p| matches!(p.message_id(), Some(id) if id == seq_id))
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

            // TODO: Put an actual packet ID
            crate_builder.set_packet_id(0);

            // Finally, our crate is ready to go. All we need to do is build and send it
            let data = crate_builder.build();
            let _ = socket.send_to(data, self.to);

            // Decrement the amount of messages we got
            available_messages -= 1;

            // Because we'll have some messages that we couldn't fit - we're going to put them back onto the candidate list
            while let Some(message) = cant_fit_stack.pop() {
                candidates.push_front(message);
            }
        }

        // If after all this we STILL have messages to send - we're going to send them next frame
        while let Some(message) = candidates.pop_back() {
            // If our message is un-acknowledged - we're not adding it back on the queue, since it's already there
            if !matches!(message.message_id(), Some(seq_id) if !self.self_acknowledged.contains(&seq_id))
            {
                continue;
            }

            self.message_queue.push_front(message);
        }

        // Don't forget to clear the acknowledged list of our messages
        self.self_acknowledged.clear();
    }

    pub fn poll(&mut self, socket: &mut SimpleSock, crate_builder: &mut PacketCrateBuilder, dt: f32) {
        // Update our RTT timers
        self.update_rtt_timers(dt);

        // Then send our own messages
        self.prepare_and_send(socket, crate_builder, dt);
    }

    /// Process the provided message (by filtering it out)
    pub fn process_message(&mut self, message: UserMessage) {
        match message.message_id() {
            Some(seq_id) => {
                if self.message_window.within_bounds(seq_id) && !self.message_window.is_marked(seq_id) {
                    self.channels.process_message(&self.message_window, message);

                    self.message_window.mark(seq_id);
                }
            }
            None => {
                self.channels.process_message(&self.message_window, message);
            }
        }
    }

    /// Receive all *available* messages
    pub fn recv_message(&mut self) -> Option<UserMessage> {
        self.channels.recv_message(&self.message_window)
    }

    /// Get the average round trip time (in seconds)
    pub fn round_trip_time(&self) -> f32 {
        self.rtt
    }

    /// Get the average packet loss (between 0 and 1)
    pub fn packet_loss(&self) -> f32 {
        let (sent, lost) = (self.packets_sent, self.packets_lost);

        // If we didn't send anything - automatically return 0.0
        if sent == 0 {
            return  0.0;
        }

        (lost as f32 / sent as f32).clamp(0.0, 1.0)
    }

    pub fn to_addr(&self) -> net::SocketAddr {
        self.to
    }
}