use crate::{
    packet::UserMessage,
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
}

impl ReceiveManager {
    pub fn new() -> Self {
        // An arbitrary number that in the future should depend on statistics instead
        let window_len = 64;

        Self {
            channels: ChannelStorage::new(),

            recv_message_window: SlidingAckWindow::new(window_len),
            recv_packet_window: SlidingAckWindow::new(window_len),
        }
    }

    /// Process the provided user message
    pub fn process_message(&mut self, message: UserMessage) {
        match message.message_id() {
            Some(packet_id) => {
                if self.recv_message_window.within_bounds(packet_id)
                    && !self.recv_message_window.is_marked(packet_id)
                {
                    self.channels
                        .process_message(&self.recv_message_window, message);

                    self.recv_message_window.mark(packet_id);
                }
            }
            None => self
                .channels
                .process_message(&self.recv_message_window, message),
        }
    }

    /// Get the window of messages that we were able to receive
    pub fn received_messages(&self) -> &SlidingAckWindow {
        &self.recv_message_window
    }

    /// Try receive a message from all our channels
    pub fn recv_message(&mut self) -> Option<UserMessage> {
        self.channels.recv_message(&self.recv_message_window)
    }
}
