use crate::{
    packet::{PacketSeqId, UserMessage},
    socket::channels::{Channel, ChannelStorage},
    window::SlidingAckWindow,
};

pub struct ReceiveManager {
    /// The channels processing received packets
    channels: ChannelStorage,

    /// Packet IDs of of messages that we have received.
    ///
    /// This window allows us to track which messages we received from the other socket. It allows us to know:
    /// - What is the oldest message we didn't receive
    /// - What messages we DID receive
    recv_message_window: SlidingAckWindow,

    /// This is somewhat similar to [ReceiveManager::recv_message_window], but it tracks packets instead.
    /// This is only useful for tracking which packets we received from the other socket.
    recv_packet_window: SlidingAckWindow,

    /// How many packets have we received during the last poll
    packets_received: usize,

    /// How many bytes have we received during the last poll
    bytes_received: usize,

    /// How many dublicates have we received during the last poll
    dublicates_received: usize,
}

impl ReceiveManager {
    pub fn new() -> Self {
        // An arbitrary number that in the future should depend on statistics instead
        let window_len = 64;

        Self {
            channels: ChannelStorage::new(),

            recv_message_window: SlidingAckWindow::new(window_len),
            recv_packet_window: SlidingAckWindow::new(window_len),

            packets_received: 0,
            bytes_received: 0,
            dublicates_received: 0,
        }
    }

    /// Process the provided user message
    pub fn process_message(&mut self, message: UserMessage) {
        match message.message_id() {
            // A reliable message
            Some(packet_id) => {
                if self.recv_message_window.within_bounds(packet_id)
                    && !self.recv_message_window.is_marked(packet_id)
                {
                    self.channels
                        .process_message(&self.recv_message_window, message);

                    self.recv_message_window.mark(packet_id);
                }
            }

            // An unreliale message
            None => self
                .channels
                .process_message(&self.recv_message_window, message),
        }
    }

    pub fn reset_immediate_stats(&mut self) {
        self.dublicates_received = 0;
        self.packets_received = 0;
        self.bytes_received = 0;
    }

    pub fn mark_received_packet_id(&mut self, packet: PacketSeqId, len: usize) {
        // Make sure to update our statistics
        self.packets_received += 1;
        self.bytes_received += len;

        if self.recv_packet_window.is_marked(packet) || self.recv_packet_window.is_old(packet) {
            self.dublicates_received += 1;
        }

        self.recv_packet_window.mark(packet);
    }

    /// Get the window of packets that we were able to receive
    pub fn received_packets_window(&self) -> &SlidingAckWindow {
        &self.recv_packet_window
    }

    /// Try receive a message from all our channels
    pub fn recv_message(&mut self) -> Option<UserMessage> {
        self.channels.recv_message(&self.recv_message_window)
    }

    /// How many dublicates have we received during the last poll.
    pub fn dublicates_received(&self) -> usize {
        self.dublicates_received
    }

    pub fn packets_received(&self) -> usize {
        self.packets_received
    }

    pub fn bytes_received(&self) -> usize {
        self.bytes_received
    }
}
