//! Channels act as filters for our packets. They serve 2 main purposes:
//! 1. Some of them maintain packet order (for example by stalling until all packets are received)
//! 2. They filter out dublicate packets (that were already received by the user)
//!
//! Some packets serve a bit more purposes than that, but the primary usecases are these.

use std::{cmp::Reverse, collections::{BTreeMap, VecDeque}};

use crate::{
    Reliability,
    packet::{PacketSeqId, UserPacket},
    window::SlidingAckWindow,
};

/// A super minimal trait for all channels
pub trait Channel {
    /// Process the provided packet  
    fn process_packet(&mut self, window: &SlidingAckWindow, packet: UserPacket);

    /// Try receive a packet (if available)
    fn recv_packet(&mut self, window: &SlidingAckWindow) -> Option<UserPacket>;
}

struct ReorderedPacket {
    packet: UserPacket,
    
    // This is reversed to ensure that we sort from the smallest to the biggest sequenced packet ID
    seq_id: Reverse<PacketSeqId>
}

impl ReorderedPacket {
    pub fn new(packet: UserPacket) -> Self {
        let seq_id = packet.sequence_id().expect("Reordered packets must always contain a sequence ID");

        Self {
            seq_id: Reverse(seq_id),
            packet,
        }
    }
}

impl PartialEq for ReorderedPacket {
    fn eq(&self, other: &Self) -> bool {
        self.seq_id.eq(&other.seq_id)
    }
}

impl PartialOrd for ReorderedPacket {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.seq_id.partial_cmp(&other.seq_id)
    }
}

/// A fully reliable channel (reliable and ordered). The slowest, but most reliable
struct ReliableChannel {
    /// The receive buffer
    recv_buff: BTreeMap<PacketSeqId, UserPacket>,
    window_pos: PacketSeqId,
}

impl ReliableChannel {
    fn new() -> Self {
        Self {
            recv_buff: BTreeMap::new(),
            window_pos: 0,
        }
    }
}

impl Channel for ReliableChannel {
    fn process_packet(&mut self, window: &SlidingAckWindow, packet: UserPacket) {
        assert_eq!(packet.reliability(), Reliability::Reliable);

        // Get the most recent window position
        self.window_pos = window.window_position();

        let seq_id = packet.sequence_id().unwrap();

        // The filter here is simple: if our packet is not yet marked - add it to the receiving buffer
        if !window.is_marked(seq_id) {
            self.recv_buff.insert(seq_id, packet);
        }
    }

    /// Try receive a user packet if possible
    fn recv_packet(&mut self, window: &SlidingAckWindow) -> Option<UserPacket> {
        if self.recv_buff.is_empty() {
            return None;
        }

        let seq_id = *self.recv_buff.first_key_value().unwrap().0;

        // If the packet's sequence ID is actually now considered "old". Only then we can receive said packet
        if seq_id < window.window_position() {
            Some(self.recv_buff.pop_first().unwrap().1)
        } else {
            None
        }
    }
}

/// A reliable channel only cares about reliability and deduplication
struct ReliableUnorderedChannel {
    recv_buff: VecDeque<UserPacket>,
}

impl ReliableUnorderedChannel {
    fn new() -> Self {
        Self {
            recv_buff: VecDeque::new(),
        }
    }
}

impl Channel for ReliableUnorderedChannel {
    fn process_packet(&mut self, window: &SlidingAckWindow, packet: UserPacket) {
        let seq_id = packet
            .sequence_id()
            .expect("Reliable packets always have sequence IDs");

        // Here we don't care about any order whatsoever

        if !window.is_marked(seq_id) {
            self.recv_buff.push_back(packet);
        }
    }

    fn recv_packet(&mut self, _: &SlidingAckWindow) -> Option<UserPacket> {
        self.recv_buff.pop_front()
    }
}

/// A reliable channel only cares about reliability and deduplication
struct UnreliableChannel {
    recv_buff: VecDeque<UserPacket>,
}

impl UnreliableChannel {
    fn new() -> Self {
        Self {
            recv_buff: VecDeque::new(),
        }
    }
}

impl Channel for UnreliableChannel {
    fn process_packet(&mut self, _: &SlidingAckWindow, packet: UserPacket) {
        self.recv_buff.push_back(packet);
    }

    fn recv_packet(&mut self, _: &SlidingAckWindow) -> Option<UserPacket> {
        self.recv_buff.pop_front()
    }
}

/// A storage of different channels
pub struct ChannelStorage {
    reliable_unordered: ReliableUnorderedChannel,
    reliable: ReliableChannel,
    unreliable: UnreliableChannel,
}

impl ChannelStorage {
    pub fn new() -> Self {
        Self {
            reliable_unordered: ReliableUnorderedChannel::new(),
            reliable: ReliableChannel::new(),
            unreliable: UnreliableChannel::new(),
        }
    }
}

impl Channel for ChannelStorage {
    fn process_packet(&mut self, window: &SlidingAckWindow, packet: UserPacket) {
        match packet.reliability() {
            Reliability::Reliable => self.reliable.process_packet(window, packet),
            Reliability::ReliableUnordered => {
                self.reliable_unordered.process_packet(window, packet)
            }
            Reliability::Unreliable => self.unreliable.process_packet(window, packet),
        }
    }

    fn recv_packet(&mut self, window: &SlidingAckWindow) -> Option<UserPacket> {
        if let Some(packet) = self.unreliable.recv_packet(window) {
            return Some(packet);
        }

        if let Some(packet) = self.reliable_unordered.recv_packet(window) {
            return Some(packet);
        }

        if let Some(packet) = self.reliable.recv_packet(window) {
            return Some(packet);
        }

        None
    }
}
