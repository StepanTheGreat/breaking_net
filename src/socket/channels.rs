//! Channels act as filters for our packets. They serve 2 main purposes:
//! 1. Some of them maintain packet order (for example by stalling until all packets are received)
//! 2. They filter out dublicate packets (that were already received by the user)
//!
//! Some packets serve a bit more purposes than that, but the primary usecases are these.

use std::collections::VecDeque;

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

/// A fully reliable channel (reliable and ordered). The slowest, but most reliable
struct ReliableChannel {
    /// The receive buffer
    recv_buff: Vec<UserPacket>,
    window_pos: PacketSeqId,
}

impl ReliableChannel {
    fn new() -> Self {
        Self {
            recv_buff: Vec::new(),
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
            self.recv_buff.push(packet);
        }
    }

    /// Try receive a user packet if possible
    fn recv_packet(&mut self, window: &SlidingAckWindow) -> Option<UserPacket> {
        if self.recv_buff.is_empty() {
            return None;
        }

        // This will simply find the packet with smallest sequence ID
        let (mn_ind, seq_id) = self
            .recv_buff
            .iter()
            .map(|p| p.sequence_id().unwrap())
            .enumerate()
            .min_by(|(_, a), (_, b)| a.cmp(b))
            .unwrap();

        // If the packet's sequence ID is actually now considered "old". Only then we can receive said packet
        if seq_id < window.window_position() {
            Some(self.recv_buff.swap_remove(mn_ind))
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
