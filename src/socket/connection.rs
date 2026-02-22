use std::{
    collections::HashMap, net,
};

use crate::{
    packet::{MessageId, PacketAckMap, PacketCrateBuilder, PacketSeqId, UserMessage},
    socket::{SimpleSock, channels::{Channel, ChannelStorage}, receiver::ReceiveManager, sender::{SendContext, SendManager}},
    window::SlidingAckWindow,
};

const PACKET_WINDOW_LEN: usize = 32;
const MESSAGE_WINDOW_LEN: usize = 64; 

/// Resend 10 times per second
const RESEND_TIMER: f32 = 1.0 / 10.0;

const RTT_SMOOTH_FACTOR: f32 = 0.4;
const RTT_MAX_TIME: f32 = 1.0;

const INIT_RTT: f32 = 0.0;

pub struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    /// The amount of packets we lost
    packets_lost: u32,

    sender: SendManager,

    receiver: ReceiveManager,

    /// A map of rtt timers
    rtt_timers: HashMap<PacketSeqId, f32>,

    /// The approximate RTT
    rtt: f32
}

impl SocketConnection {
    pub fn new(to: net::SocketAddr) -> Self {
        let sender = SendManager::new(to, 100);
        let receiver = ReceiveManager::new();

        Self {
            to,
            packets_lost: 0,
            
            sender,
            receiver,

            rtt_timers: HashMap::new(),
            rtt: INIT_RTT
        }
    }

    /// Acknowledgments for my packets have been received on this connection
    pub fn sent_packet_acknowledgments_received(&mut self, ack_base: MessageId, ack_map: PacketAckMap) {
        // No acknowledgments
        if ack_base == 0 && ack_map == 0 {
            return;
        }
        
        // Init the cursor
        let mut cursor = 1 << (PacketAckMap::BITS-1);

        // For each bit
        for bind in 0..PacketAckMap::BITS {
            if (ack_map & cursor) > 0 {

                let packet_id = ack_base + bind;

                self.my_packets_acknowledged.insert(packet_id);
                self.mark_rtt_received(packet_id);
            } 
            
            // Move the cursor to the right
            cursor >>=  1;
        }
    }

    pub fn other_packet_acknowledgment_received(&mut self, ack: MessageId) {
        self.other_packets_acknowledged.insert(ack);
    }

    /// Update RTT timers and when some of them are maxed out - remove them 
    fn update_rtt_timers(&mut self, dt: f32) {
        self.rtt_timers.retain(|_, timer| {
            *timer = (*timer + dt).min(1.0);

            // If our message timed out
            if *timer == RTT_MAX_TIME {

                // We would also like to increment the packet loss counter
                self.packets_lost += 1;

                false
            } else {
                true
            }

        });
    }

    /// Mark this sequence ID as received in the RTT calculations
    fn mark_rtt_received(&mut self, packet_id: MessageId) {
        // If it's actually present - we're going to pop it

        if let Some(time) = self.rtt_timers.remove(&packet_id) {
            // Update our rtt according to the smoothed average formula
            
            self.rtt += RTT_SMOOTH_FACTOR*(time-self.rtt);
        }
    }

    /// Add an RTT tracker 
    fn add_rtt_tracker(&mut self, ack: MessageId) {
        self.rtt_timers.insert(ack, 0.0);
    }

    pub fn poll(&mut self, socket: &mut SimpleSock, crate_builder: &mut PacketCrateBuilder, dt: f32) {
        // Update our RTT timers
        self.update_rtt_timers(dt);

        // Poll our sender
        self.sender.poll(
            SendContext {
                socket: socket,
                packet_builder: crate_builder,
                recv_packet_window: &self.recv_packet_window
            }, 
            dt
        );
    }

    /// Process the provided message (by filtering it out)
    pub fn process_message(&mut self, message: UserMessage) {
        match message.message_id() {
            Some(packet_id) => {
                if self.message_window.within_bounds(packet_id) && !self.message_window.is_marked(packet_id) {
                    self.channels.process_message(&self.message_window, message);

                    self.message_window.mark(packet_id);
                }
            }
            None => {
                self.channels.process_message(&self.message_window, message);
            }
        }
    }

    /// Receive all *available* messages
    pub fn recv_message(&mut self) -> Option<UserMessage> {
        self.channels.recv_message(&self.message_window)
    }

    /// Get the average round trip time (in seconds)
    pub fn round_trip_time(&self) -> f32 {
        self.rtt
    }

    /// Get the average packet loss (between 0 and 1)
    pub fn packet_loss(&self) -> f32 {
        let (sent, lost) = (self.packets_sent, self.packets_lost);

        // If we didn't send anything - automatically return 0.0
        if sent == 0 {
            return  0.0;
        }

        (lost as f32 / sent as f32).clamp(0.0, 1.0)
    }

    pub fn to_addr(&self) -> net::SocketAddr {
        self.to
    }
}