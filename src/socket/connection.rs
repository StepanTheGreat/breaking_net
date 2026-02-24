use std::net;

use crate::{
    Reliability,
    packet::{MessageAckMap, MessageId, PacketAckMap, PacketCrateBuilder, UserMessage},
    socket::{
        SimpleSock,
        receiver::ReceiveManager,
        sender::{SendContext, SendManager},
    },
};

const PACKET_WINDOW_LEN: usize = 32;
const MESSAGE_WINDOW_LEN: usize = 64;

/// Resend 10 times per second
const RESEND_TIMER: f32 = 1.0 / 10.0;

pub struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    sender: SendManager,

    receiver: ReceiveManager,
}

impl SocketConnection {
    pub fn new(to: net::SocketAddr) -> Self {
        let sender = SendManager::new(to, 100);
        let receiver = ReceiveManager::new();

        Self {
            to,
            sender,
            receiver,
        }
    }

    /// Acknowledgments for my messages have been received on this connection
    pub fn sent_message_acknowledgments_received(
        &mut self,
        ack_base: MessageId,
        ack_map: MessageAckMap,
    ) {
        // No acknowledgments
        if ack_base == 0 && ack_map == 0 {
            return;
        }

        // Init the cursor
        let mut cursor = 1 << (MessageAckMap::BITS - 1);

        // For each bit
        for bind in 0..MessageAckMap::BITS {
            if (ack_map & cursor) > 0 {
                let msg_id = ack_base + bind;
                self.receiver.mark_sent_message_received(msg_id);
            }

            // Move the cursor to the right
            cursor >>= 1;
        }
    }

    pub fn poll(
        &mut self,
        socket: &mut SimpleSock,
        crate_builder: &mut PacketCrateBuilder,
        dt: f32,
    ) {
        // Poll our sender
        self.sender.poll(
            SendContext {
                socket: socket,
                packet_builder: crate_builder,
                recv_packet_window: self.receiver.received_messages(),
            },
            dt,
        );
    }

    /// Process the provided message (by filtering it out)
    pub fn process_message(&mut self, message: UserMessage) {
        self.receiver.process_message(message);
    }

    /// Receive all *available* messages
    pub fn recv_message(&mut self) -> Option<UserMessage> {
        self.receiver.recv_message()
    }

    /// Queue a new message to send
    pub fn queue_message(&mut self, payload: Vec<u8>, reliability: Reliability) {
        self.sender.queue_msg(payload, reliability);
    }

    pub fn to_addr(&self) -> net::SocketAddr {
        self.to
    }
}
