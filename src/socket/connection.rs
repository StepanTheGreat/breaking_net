use std::{net, time::Duration};

use crate::{
    Reliability,
    packet::{MessageAckMap, PacketAckMap, PacketCrateBuilder, PacketSeqId, UserMessage},
    socket::{
        SocketBackend,
        receiver::ReceiveManager,
        sender::{SendContext, SendManager},
    },
};

/// After how many seconds to time out without receiving any packets
const HEARBEAT_TIMEOUT: Duration = Duration::from_millis(5_000);

pub struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    sender: SendManager,

    receiver: ReceiveManager,

    last_hearbeat: Duration,

    time: Duration,
}

impl SocketConnection {
    pub fn new(to: net::SocketAddr, packets_per_second: u32) -> Self {
        let time = Duration::ZERO;
        let sender = SendManager::new(time, to, packets_per_second);
        let receiver = ReceiveManager::new();

        Self {
            to,
            sender,
            receiver,
            time,
            last_hearbeat: time + HEARBEAT_TIMEOUT,
        }
    }

    pub fn reset_heartbeat_timer(&mut self) {
        self.last_hearbeat = self.time + HEARBEAT_TIMEOUT;
    }

    /// Acknowledgments for our messages have been received on this connection
    pub fn sent_packet_acknowledgments_received(
        &mut self,
        packet_base: PacketSeqId,
        packet_map: PacketAckMap,
    ) {
        // No acknowledgments
        if packet_base == 0 && packet_map == 0 {
            return;
        }

        self.sender.set_sent_packet_received_base(packet_base);

        // Init the cursor
        let mut cursor = 1 << (MessageAckMap::BITS - 1);

        // For each bit
        for bind in 0..MessageAckMap::BITS {
            if (packet_map & cursor) > 0 {
                let packet_id = packet_base + bind;
                self.sender.mark_sent_packet_received(packet_id);
            }

            // Move the cursor to the right
            cursor >>= 1;
        }
    }

    pub fn poll(
        &mut self,
        socket: &mut dyn SocketBackend,
        crate_builder: &mut PacketCrateBuilder,
        dt: Duration,
    ) {
        // Update our total time
        self.time += dt;

        // Poll our sender
        self.sender.poll(
            SendContext {
                socket,
                packet_builder: crate_builder,
                recv_packet_window: self.receiver.received_packets(),
            },
            self.time,
        );
    }

    /// Mark this recipient's sent packet as received
    pub fn mark_received_packet_id(&mut self, packet: PacketSeqId) {
        self.receiver.mark_received_packet_id(packet);
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
        self.last_hearbeat <= self.time
    }

    pub fn rtt(&self) -> f64 {
        self.sender.rtt()
    }

    pub fn packet_loss(&self) -> f64 {
        self.sender.packet_loss()
    }
}
