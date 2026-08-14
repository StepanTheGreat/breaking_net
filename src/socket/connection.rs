use std::{net, time::Duration};

use crate::{
    Reliability, packet::{
        PacketAckMap, PacketCrate, PacketCrateBuilder, PacketSeqId, UserMessage,
    }, socket::{
        SocketBackend,
        receiver::ReceiveManager,
        sender::{SendContext, SendManager},
        stats::{AdvancedConnectionStats, ConnectionStats},
    }, utils::{Circular, Time},
};

/// After how many seconds to time out without receiving any packets
const HEARBEAT_TIMEOUT: Duration = Duration::from_millis(7_500);

pub struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    sender: SendManager,

    receiver: ReceiveManager,

    last_hearbeat: Duration,

    time: Time,

    advanced_stats: Circular<AdvancedConnectionStats>,
}

impl SocketConnection {
    pub fn new(to: net::SocketAddr, packets_per_second: u32) -> Self {
        let time = Time::new();
        let sender = SendManager::new(time.clone(), to, packets_per_second);
        let receiver = ReceiveManager::new();

        Self {
            to,
            sender,
            receiver,
            last_hearbeat: time.elapsed() + HEARBEAT_TIMEOUT,
            time,
            advanced_stats: Circular::new(10),
        }
    }

    fn reset_heartbeat_timer(&mut self) {
        self.last_hearbeat = self.time.elapsed() + HEARBEAT_TIMEOUT;
    }

    /// Acknowledgments for our packets have been received on this connection
    fn sent_packet_acknowledgments_received(
        &mut self,
        packet_base: PacketSeqId,
        packet_map: PacketAckMap,
    ) {
        // No acknowledgments
        if packet_base == 0 && packet_map == 0 {
            return;
        }

        let dt = self.time.delta();

        // Init the cursor
        let mut cursor = 1;

        // For each bit
        for bind in 0..PacketAckMap::BITS {
            if (packet_map & cursor) > 0 {
                let packet_id = packet_base + bind;
                self.sender.mark_sent_packet_received(packet_id, dt);
            }

            // Move the cursor to the leeft
            cursor <<= 1;
        }
    }

    pub fn process_packet(&mut self, pcrate: PacketCrate, bytes_len: usize) {
        // Some packets are ack-only, don't acknowledge those
        if let Some(seq_id) = pcrate.seq_id {
            self.receiver.mark_received_packet_id(seq_id, bytes_len);
        }

        // push our new packet score
        self.sender.push_new_packet_score(pcrate.packet_score_id, pcrate.packet_score);

        // let it mark all the acknowledgments it needs
        self.sent_packet_acknowledgments_received(pcrate.packet_base, pcrate.packet_map);

        // and reset its hearbeat timer as well
        self.reset_heartbeat_timer();

        for message in pcrate.messages {
            // Process it (filter, reorder it and so on)
            self.receiver.process_message(message);
        }
    }

    /// Update this time (should be called before processing any packets)
    pub fn update_time(&self, dt: Duration) {
        self.time.tick(dt);
    }

    pub fn poll(
        &mut self,
        socket: &mut dyn SocketBackend,
        crate_builder: &mut PacketCrateBuilder,
    ) {
        // Poll our sender
        self.sender.poll(
            SendContext {
                socket,
                packet_builder: crate_builder,
                recv_packet_window: self.receiver.received_packets_window(),
            }
        );
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

    /// Reset all immediate statistics. Should be called once after each poll.
    pub fn reset_immediate_stats(&mut self) {
        self.receiver.reset_immediate_stats();
        self.sender.reset_immediate_stats();
    }

    pub fn record_immediate_stats(&mut self) {
        self.advanced_stats.push(self.advanced_statistics());
    }

    /// Check if this connection has timed out (no packets received)
    pub fn timed_out(&self) -> bool {
        self.last_hearbeat <= self.time.elapsed()
    }

    /// Get a complete snapshot of all the statistics of this connection
    pub fn statistics(&self) -> ConnectionStats {
        ConnectionStats {
            rtt: self.sender.rtt(),
            median_rtt: self.sender.base_rtt(),
            packet_loss: self.sender.packet_loss(),
            jitter: self.sender.rtt_deviation(),
        }
    }

    /// Advanced statistics (that record everything during a single poll)
    pub fn advanced_statistics(&self) -> AdvancedConnectionStats {
        AdvancedConnectionStats {
            queued_messages: self.sender.queued_messages(),
            packets_sent: self.sender.packets_sent(),
            bytes_sent: self.sender.bytes_sent(),
            dublicates_received: self.receiver.dublicates_received(),
            packets_received: self.receiver.packets_received(),
            bytes_received: self.receiver.bytes_received(),
            packets_lost: self.sender.packets_lost(),
        }
    }

    pub fn avg_advanced_statistics(&self) -> AdvancedConnectionStats {
        self.advanced_stats.average()
    }
}
