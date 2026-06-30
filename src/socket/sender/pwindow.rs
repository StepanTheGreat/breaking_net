use std::{collections::VecDeque, time::Duration};

use crate::{
    packet::{MessageId, PacketSeqId},
    socket::{
        sender::{INIT_PACKET_LOSS, PACKET_LOSS_ALPHA},
        stats::Ema,
    },
    utils::StackVec,
};

/// A single packet entry
pub struct PacketWindowEntry {
    pub timestamp: Duration,

    pub messages: StackVec<MessageId, 4>,
}

impl PacketWindowEntry {
    pub fn new(timestamp: Duration, messages: StackVec<MessageId, 4>) -> Self {
        Self {
            timestamp,
            messages,
        }
    }
}

/// A sliding packet window which lets us sent packets by us. They include important information such as:
/// - Timestamp of each packet
/// - A list of messages associated to each packet (more efficient to track)
pub struct PacketWindow {
    /// A queue of packets
    queue: VecDeque<Option<PacketWindowEntry>>,

    /// The current position of the window. The top packet in the queue has this ID
    pos: PacketSeqId,

    /// Auto incrementing packet ID
    next_packet_id: PacketSeqId,

    /// EMA packet loss, useful for general statistics
    packet_loss: Ema,

    /// How many packets have we lost since the last poll
    packets_lost: usize,

    capacity: usize,
}

impl PacketWindow {
    /// Create a new window at position 0
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            pos: capacity as u32,
            next_packet_id: 0,

            packet_loss: Ema::new(INIT_PACKET_LOSS, PACKET_LOSS_ALPHA),
            packets_lost: 0,
            capacity,
        }
    }

    /// Get the base of the window (included in the queue)
    pub fn window_base(&self) -> u32 {
        self.pos - (self.capacity as u32)
    }

    /// Get the current window position (ceiling). The ceiling is not included in the map, it's rather the next packet to get sent.
    pub fn window_position(&self) -> PacketSeqId {
        self.pos
    }

    /// Get the next packet ID
    fn next_pid(&self) -> PacketSeqId {
        self.next_packet_id
    }

    /// Try map a packet ID to its index in the window
    ///
    /// None means that the id is unreachable (too old or recent)
    fn pid_to_ind(&self, id: PacketSeqId) -> Option<usize> {
        let base = self.window_base();

        // If ID is too old OR its outside our window - return None
        if id < base || id >= self.pos {
            None
        } else {
            Some((id - base) as usize)
        }
    }

    /// Push a new packet onto the window.
    ///
    /// # Panics
    /// If the id was unexpected. All packet entries must match their sequential IDs
    pub fn push_sent(&mut self, id: PacketSeqId, entry: PacketWindowEntry) {
        assert_eq!(self.next_pid(), id, "Unexpected packet ID");

        // Make sure to drop overflowing packets
        if self.queue.len() == self.capacity {
            let packet = self.queue.pop_front().unwrap();

            // If a packet wasn't acknowledged - mark it as lost
            if packet.is_some() {
                self.add_packet_loss_status(true);
            }
        }

        // Increment our next packet ID
        self.next_packet_id += 1;

        // Move our window
        self.pos = self.pos.max(self.next_packet_id);

        // Push our packet
        self.queue.push_back(Some(entry));
    }

    /// Mark next packet status to compute packet loss
    fn add_packet_loss_status(&mut self, lost: bool) {
        if lost {
            self.packet_loss.push(1.0);
            self.packets_lost += 1;
        } else {
            self.packet_loss.push(0.0);
        }
    }

    /// Try retrieve and mark the provided packet ID as received.
    ///
    /// This will remove it from the window and slide it.
    ///
    /// It will return [None] if the packet is out of reach or was already taken
    pub fn mark_sent(&mut self, id: PacketSeqId) -> Option<PacketWindowEntry> {
        let ind = self.pid_to_ind(id)?;

        // Take the packet our
        let packet = self.queue[ind].take();

        if packet.is_some() {
            // Mark our packet as not lost
            self.add_packet_loss_status(false);
        }

        packet
    }

    pub fn reset_immediate_stats(&mut self) {
        self.packets_lost = 0;
    }

    /// Get current packet loss (from 0 to 1)
    pub fn packet_loss(&self) -> f64 {
        self.packet_loss.get()
    }

    /// How many packets were lost during the last poll
    pub fn packets_lost(&self) -> usize {
        self.packets_lost
    }

    /// Count how many packets are in flight, or essentially unacknowledged.
    pub fn in_flight(&self) -> usize {
        let mut total = 0;

        for packet in self.queue.iter() {
            if packet.is_some() {
                total += 1;
            }
        }

        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::socket::sender::*;

    const DT: Duration = Duration::from_millis(16);

    #[test]
    fn test_packet_window() {
        let mut time = Duration::ZERO;

        let mut window = PacketWindow::new(4);

        assert_eq!(window.window_position(), 4);
        assert_eq!(window.window_base(), 0);

        let timestamp0 = time;
        // Packet 0
        window.push_sent(0, PacketWindowEntry::new(timestamp0, vec![0, 1, 2].into()));
        time += DT;

        // Packet 1
        let timestamp1 = time;
        window.push_sent(1, PacketWindowEntry::new(timestamp1, vec![1, 2, 3].into()));
        time += DT;

        // Packet 2
        let timestamp2 = time;
        window.push_sent(2, PacketWindowEntry::new(timestamp2, vec![2, 3, 4].into()));
        time += DT;

        let timestamp3 = time;
        window.push_sent(3, PacketWindowEntry::new(timestamp3, vec![3, 4, 5].into()));
        time += DT;

        // We added 4 packets, the window's position and base should stay the same
        assert_eq!(window.window_position(), 4);
        assert_eq!(window.window_base(), 0);

        // Say we received packet 1
        {
            let p = window.mark_sent(1).unwrap();
            assert_eq!(p.timestamp, timestamp1);
            assert_eq!(p.messages.as_slice(), &[1, 2, 3]);
            assert_eq!(time - p.timestamp, DT * 3);
        }

        // Update the time slightly again
        time += DT;

        // Now we received packet 2
        {
            let p = window.mark_sent(2).unwrap();
            assert_eq!(p.timestamp, timestamp2);
            assert_eq!(p.messages.as_slice(), &[2, 3, 4]);
            assert_eq!(time - p.timestamp, DT * 3);
        }

        // We'll add a final packet
        let timestamp4 = time;
        window.push_sent(4, PacketWindowEntry::new(timestamp4, vec![4, 5, 6].into()));
        time += DT;

        // The window has now overflowed, so we probably lost the last packet
        assert_eq!(window.window_position(), 5);
        assert_eq!(window.window_base(), 1);

        // We lost one packet
        assert_eq!(window.packets_lost(), 1);
    }
}
