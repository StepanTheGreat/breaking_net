use rand::seq::{self, SliceRandom};
use std::{
    collections::{HashMap, HashSet, VecDeque}, net, rc::Rc
};

use crate::{
    packet::{PacketAckMap, PacketCrateBuilder, PacketSeqId, Reliability, UserPacket, build_ack_map},
    socket::{SimpleSock, channels::{Channel, ChannelStorage}},
    window::SlidingAckWindow,
};

/// Resend 15 times per second
const RESEND_TIMER: f32 = 1.0 / 15.0;

const RTT_SMOOTH_FACTOR: f32 = 0.4;
const RTT_MAX_TIME: f32 = 1.0;

const INIT_RTT: f32 = 0.0;

/// A super simple sequence counter, that just increments and wrap arounds sequence ids
struct SequenceCounter(PacketSeqId);
impl SequenceCounter {
    fn new() -> Self {
        Self(0)
    }

    /// Cycle the next value
    fn next(&mut self) -> PacketSeqId {
        let next = self.0;
        self.0 = self.0.wrapping_add(1);

        next
    }
}

#[derive(Clone)]
struct QueuedPacket {
    packet: UserPacket,
    timer: Option<f32>
}

impl QueuedPacket {
    fn new_reliable(packet: UserPacket, timer: f32) -> Self {
        Self {
            packet,
            timer: Some(timer)
        }
    }

    fn new_unreliable(packet: UserPacket) -> Self {
        Self {
            packet, 
            timer: None
        }
    }

    fn tick(&mut self, dt: f32) {
        if let Some(timer) = self.timer.as_mut() {
            *timer = (*timer - dt).max(0.0);
        }
    }

    /// Is this queued packet ready?
    fn is_ready(&self) -> bool {
        self.timer.unwrap_or(0.0) == 0.0
    }

    fn size(&self) -> usize {
        self.packet.size()
    }

    fn sequence_id(&self) -> Option<PacketSeqId> {
        self.packet.sequence_id()
    }

    fn consume(self) -> UserPacket {
        self.packet
    }

    /// Update this packet's timer
    fn set_timer(&mut self, new_time: f32) {
        if let Some(timer) = self.timer.as_mut() {
            *timer = new_time;
        }
    }
}

pub struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    /// The amount of packets per second
    packets_per_second: usize,

    /// Packets to send with their respected decrementing timers
    /// A queue of packets
    packet_queue: VecDeque<QueuedPacket>,

    /// The counter to obtain sequence IDs from
    seq_counter: SequenceCounter,

    channels: ChannelStorage,

    /// The sliding window for all reliable packets
    packet_window: SlidingAckWindow,

    /// Sequence IDs of packets that were **sent** by us
    self_acknowledged: HashSet<PacketSeqId>,

    /// Sequence IDs of packets that were **received** by us
    other_acknowledged: HashSet<PacketSeqId>,

    /// The amount packets we sent
    packets_sent: u32,

    /// The amount of packets we lost
    packets_lost: u32,

    /// A map of rtt timers
    rtt_timers: HashMap<PacketSeqId, f32>,

    /// The approximate RTT
    rtt: f32
}

impl SocketConnection {
    pub fn new(to: net::SocketAddr) -> Self {
        let packets_per_second = 100;

        Self {
            to,

            packets_per_second,
            
            packet_queue: VecDeque::new(),
            seq_counter: SequenceCounter::new(),

            packet_window: SlidingAckWindow::new(128),
            channels: ChannelStorage::new(),

            self_acknowledged: HashSet::new(),
            other_acknowledged: HashSet::new(),

            packets_sent: 0,
            packets_lost: 0,

            rtt_timers: HashMap::new(),
            rtt: INIT_RTT
        }
    }

    /// Queue a new packet to send through this connection ASAP
    pub fn queue_packet(&mut self, reliability: Reliability, payload: Vec<u8>) {
        let payload = Rc::new(payload);

        // Based on different reliability, we're going to queue them differently
        match reliability {
            // Reliable ordered/unordered get themselves resend timers
            Reliability::Reliable | Reliability::ReliableUnordered => {
                let seq_id = self.seq_counter.next();

                let packet = if reliability == Reliability::Reliable {
                    UserPacket::new_reliable(seq_id, payload)
                } else {
                    UserPacket::new_reliable_unordered(seq_id, payload)
                };

                // Insert a new packet that must be dispatched ASAP
                self.packet_queue
                    .push_back(QueuedPacket::new_reliable(packet, 0.0));
            }

            // Unreliable however don't get themselves anything
            Reliability::Unreliable => {
                // Just push a basic unreliable packet
                self.packet_queue
                    .push_back(QueuedPacket::new_unreliable(UserPacket::new_unreliable(payload)));
            }
        }
    }

    /// Acknowledgments have been received on this connection
    pub fn own_acknowledgments_received(&mut self, ack_base: PacketSeqId, ack_map: PacketAckMap) {
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

    pub fn other_acknowledgment_received(&mut self, ack: PacketSeqId) {
        self.other_acknowledged.insert(ack);
    }


    /// Update packets while also collecting them into a queue at the same time
    fn update_collect_candidates(&mut self, dt: f32, candidates: &mut VecDeque<QueuedPacket>) {
        // We're going to go from back to front
        for ind in (0..self.packet_queue.len()).rev() {
            // First we're going to update it
            self.packet_queue[ind].tick(dt);

            // Then clone it
            let packet = self.packet_queue[ind].clone();

            // If the packet is both acknowledged and ready - remove it from the queue
            if !(matches!(packet.sequence_id(), Some(seq_id) if !self.self_acknowledged.contains(&seq_id)) || !packet.is_ready())
            {
                self.packet_queue.remove(ind);
            }

            // And if ready - add to the candidate list
            if packet.is_ready() {
                candidates.push_front(packet);
            }
        }

        #[cfg(feature = "stress_testing")]
        {
            use crate::socket::should_reorder_packets;

            if should_reorder_packets() {
                use crate::socket::RNG_STATE;

                let (a, b) = candidates.as_mut_slices();

                // All this ugly code to essentially simply shuffle this packet queue
                RNG_STATE.with(|rng| a.shuffle(&mut *rng.borrow_mut()));
                RNG_STATE.with(|rng| b.shuffle(&mut *rng.borrow_mut()));
            }
        }
    }

    /// Update RTT timers and when some of them are maxed out - remove them 
    fn update_rtt_timers(&mut self, dt: f32) {
        self.rtt_timers.retain(|_, timer| {
            *timer = (*timer + dt).min(1.0);

            // If our packed timed out
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
    fn mark_rtt_received(&mut self, seq_id: PacketSeqId) {
        // If it's actually present - we're going to pop it

        if let Some(time) = self.rtt_timers.remove(&seq_id) {
            // Update our rtt according to the smoothed average formula
            
            self.rtt += RTT_SMOOTH_FACTOR*(time-self.rtt);
        }
    }

    /// Add an RTT tracker 
    fn add_rtt_tracker(&mut self, ack: PacketSeqId) {
        self.rtt_timers.insert(ack, 0.0);
    }

    /// A separate polling method that specialises in sending packets
    fn prepare_and_send(
        &mut self, 
        socket: &mut SimpleSock, 
        crate_builder: &mut PacketCrateBuilder, 
        dt: f32
    ) {
        let mut candidates = VecDeque::with_capacity(self.packet_queue.len());
        self.update_collect_candidates(dt, &mut candidates);

        let mut cant_fit_stack = Vec::new();

        // How many packets can we even send?
        let mut available_packets = (
            self.packets_per_second as f32 * dt.clamp(0.0, 1.0)
            // No matter the delta here, we're not going to send more than our PPS in a single second
        ) as usize;

        // Add ONE acknowledgment ID into our table
        for packet in candidates.iter() {
            if let Some(seq_id) = packet.sequence_id() {
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
            let mut acknowledgments: Vec<PacketSeqId> = self.other_acknowledged.iter().copied().collect();
            acknowledgments.sort();

            build_ack_map(&acknowledgments)
        };

        // Only keep acknowledgments that didn't fit into our acknowledgment map
        self.other_acknowledged.retain(|seq_id| *seq_id > ack_base+PacketAckMap::BITS);

        // While we have some available packet slots
        while available_packets > 0 {
            // If there are no packets nor acks to send - we'll stop right here
            if candidates.is_empty() && ack_map == 0 {
                break;
            }

            // Put our acknowledgments
            crate_builder.put_acknowledgments(ack_base, ack_map);

            // While the candidate list is not empty
            while !candidates.is_empty() {
                // Extract the packet
                let packet = candidates.pop_front().unwrap();

                // If our crate can fit our packet - put it
                if crate_builder.can_fit(packet.size()) {
                    // If our packet is unacknowledged - we're going to reset its timer
                    if let Some(seq_id) = packet.sequence_id() {
                        if self.self_acknowledged.contains(&seq_id) {
                            continue;
                        }

                        // Find it and reset its timer
                        self.packet_queue
                            
                            .iter_mut()
                            .find(|p| matches!(p.sequence_id(), Some(id) if id == seq_id))
                            .unwrap()
                            .set_timer(RESEND_TIMER);
                    }

                    // Consume and push it
                    crate_builder.put_user_packet(packet.consume());
                } else {
                    // In any other case - put it in the for-later stack
                    cant_fit_stack.push(packet);
                }
            }

            // Now that we fit all our available packets - let's try to fit some acknowledgments

            // Finally, our crate is ready to go. All we need to do is build and send it
            let data = crate_builder.build();
            let _ = socket.send_to(data, self.to);

            // Decrement the amount of packets we got
            available_packets -= 1;

            // Because we'll have some packets that we couldn't fit - we're going to put them back onto the candidate list
            while let Some(packet) = cant_fit_stack.pop() {
                candidates.push_front(packet);
            }
        }

        // If after all this we STILL have packets to send - we're going to send them next frame
        while let Some(packet) = candidates.pop_back() {
            // If our packet is un-acknowledged - we're not adding it back on the queue, since it's already there
            if !matches!(packet.sequence_id(), Some(seq_id) if !self.self_acknowledged.contains(&seq_id))
            {
                continue;
            }

            self.packet_queue.push_front(packet);
        }

        // Don't forget to clear the acknowledged list of our packets
        self.self_acknowledged.clear();
    }

    pub fn poll(&mut self, socket: &mut SimpleSock, crate_builder: &mut PacketCrateBuilder, dt: f32) {
        // Update our RTT timers
        self.update_rtt_timers(dt);

        // Then send our own packets
        self.prepare_and_send(socket, crate_builder, dt);
    }

    /// Process the provided packet (by filtering it out)
    pub fn process_packet(&mut self, packet: UserPacket) {
        match packet.sequence_id() {
            Some(seq_id) => {
                if self.packet_window.within_bounds(seq_id) && !self.packet_window.is_marked(seq_id) {
                    self.channels.process_packet(&self.packet_window, packet);

                    self.packet_window.mark(seq_id);
                }
            }
            None => {
                self.channels.process_packet(&self.packet_window, packet);
            }
        }
    }

    /// Receive all *available* packets
    pub fn recv_packet(&mut self) -> Option<UserPacket> {
        self.channels.recv_packet(&self.packet_window)
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