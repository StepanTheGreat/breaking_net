use std::{net, time::Duration};

use crate::{
    Reliability, Timer,
    packet::{MessageAckMap, MessageId, PacketCrateBuilder, UserMessage},
    socket::{
        SimpleSock,
        receiver::ReceiveManager,
        sender::{SendContext, SendManager},
    },
};

/// After how many seconds to time out without receiving any packets
const MAX_HEARBEAT_TIME: Duration = Duration::from_millis(5_000);

pub struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    sender: SendManager,

    receiver: ReceiveManager,

    last_hearbeat: Timer,
}

impl SocketConnection {
    pub fn new(to: net::SocketAddr) -> Self {
        let sender = SendManager::new(to, 100);
        let receiver = ReceiveManager::new();

        Self {
            to,
            sender,
            receiver,
            last_hearbeat: Timer::new(MAX_HEARBEAT_TIME),
        }
    }

    pub fn reset_heartbeat_timer(&mut self) {
        self.last_hearbeat.set_time(MAX_HEARBEAT_TIME);
    }

    /// Acknowledgments for our messages have been received on this connection
    pub fn sent_message_acknowledgments_received(
        &mut self,
        msg_base: MessageId,
        msg_map: MessageAckMap,
    ) {
        // No acknowledgments
        if msg_base == 0 && msg_map == 0 {
            return;
        }

        self.sender.set_send_message_received_base(msg_base);

        // Init the cursor
        let mut cursor = 1 << (MessageAckMap::BITS - 1);

        // For each bit
        for bind in 0..MessageAckMap::BITS {
            if (msg_map & cursor) > 0 {
                let msg_id = msg_base + bind;
                self.sender.mark_sent_message_received(msg_id);
            }

            // Move the cursor to the right
            cursor >>= 1;
        }
    }

    pub fn poll(
        &mut self,
        socket: &mut SimpleSock,
        crate_builder: &mut PacketCrateBuilder,
        dt: Duration,
    ) {
        // Tick our heartbeat timer
        self.last_hearbeat.tick(dt);

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

    /// Check if this connection has timed out (no packets received)
    pub fn timed_out(&self) -> bool {
        self.last_hearbeat.timed_out()
    }
}
