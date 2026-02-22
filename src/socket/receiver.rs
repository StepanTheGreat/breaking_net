use crate::{socket::channels::ChannelStorage, window::SlidingAckWindow};

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

    /// Sequence IDs of messages that were sent by us. Useful for knowing which messages that we sent were actually delievered
    send_message_window: SlidingAckWindow,
}

impl ReceiveManager {
    pub fn new() -> Self {
        // An arbitrary number that in the future should depend on statistics instead
        let window_len = 64;

        Self {
            channels: ChannelStorage::new(),

            recv_message_window: SlidingAckWindow::new(window_len),
            recv_packet_window: SlidingAckWindow::new(window_len),
            send_message_window: SlidingAckWindow::new(window_len)
        }
    }

    pub fn process_packet(&mut self) {
        todo!()
    }
}