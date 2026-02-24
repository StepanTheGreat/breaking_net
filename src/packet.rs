use bitcode::{Decode, Encode};
use std::rc::Rc;

use crate::window::SlidingAckWindow;

/// An ID of a packet
pub type PacketSeqId = u32;

/// An ID of a uniquely identifiable message. The difference between the packet, is that a packet is only sent once. A message however, can be
/// resent until it's actually received. This is an important separation when measuring RTT and other metrics.
pub type MessageId = u32;

/// The message data itself
pub type MessagePayload = Rc<Vec<u8>>;

pub type PacketAckMap = u32;

pub type MessageAckMap = u32;

/// Different kinds of reliability
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reliability {
    /// A message is fully unreliable
    Unreliable,

    /// Messages are resent, but they arrive in undefined order
    ReliableUnordered,

    /// Messages arrive and get processed in the same order they were sent
    Reliable,
}

#[derive(Clone, Encode, Decode)]
pub enum UserMessageKind {
    Unreliable,
    ReliableUnordered { msg_id: MessageId },
    Reliable { msg_id: MessageId },
}

#[derive(Clone, Encode, Decode)]
pub struct UserMessage {
    kind: UserMessageKind,
    payload: MessagePayload,
}

impl UserMessage {
    fn new(kind: UserMessageKind, payload: MessagePayload) -> Self {
        Self { kind, payload }
    }

    pub fn new_reliable(msg_id: MessageId, payload: MessagePayload) -> Self {
        Self::new(UserMessageKind::Reliable { msg_id }, payload)
    }

    pub fn new_reliable_unordered(msg_id: MessageId, payload: MessagePayload) -> Self {
        Self::new(UserMessageKind::ReliableUnordered { msg_id }, payload)
    }

    pub fn new_unreliable(payload: MessagePayload) -> Self {
        Self::new(UserMessageKind::Unreliable, payload)
    }

    pub fn is_reliable(&self) -> bool {
        match self.kind {
            UserMessageKind::Reliable { .. } => true,
            UserMessageKind::ReliableUnordered { .. } => true,
            UserMessageKind::Unreliable => false,
        }
    }

    /// A conservative estimate of the total message size
    pub fn size(&self) -> usize {
        // The cost of the payload (length + data)
        let payload_size = size_of::<u32>() + self.payload.len();

        // The cost of the sequence ID in reliable messages
        let seq_id_size = match self.kind {
            UserMessageKind::ReliableUnordered { .. } => size_of::<PacketSeqId>(),
            UserMessageKind::Reliable { .. } => size_of::<PacketSeqId>(),
            UserMessageKind::Unreliable => 0,
        };

        // The cost of the enum tag for our message
        let tag_size = 1;

        tag_size + payload_size + seq_id_size
    }

    /// Get a message id from this message, if reliable
    pub fn message_id(&self) -> Option<PacketSeqId> {
        match self.kind {
            UserMessageKind::Reliable { msg_id, .. } => Some(msg_id),
            UserMessageKind::ReliableUnordered { msg_id, .. } => Some(msg_id),
            UserMessageKind::Unreliable => None,
        }
    }

    /// Get this message's reliability value
    pub fn reliability(&self) -> Reliability {
        match self.kind {
            UserMessageKind::Reliable { .. } => Reliability::Reliable,
            UserMessageKind::ReliableUnordered { .. } => Reliability::ReliableUnordered,
            UserMessageKind::Unreliable => Reliability::Unreliable,
        }
    }

    /// Consume this message's payload. This will return [None] if it still has active references
    pub fn consume_payload(self) -> Option<Vec<u8>> {
        Rc::into_inner(self.payload)
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

/// The inherent serialisation type behind packet crate
#[derive(Encode, Decode)]
pub struct PacketCrate {
    /// The sequence ID of this packet
    pub seq_id: PacketSeqId,

    /// The base of the message window
    pub msg_base: MessageId,

    /// The bitmap of the message window
    pub msg_map: MessageAckMap,

    /// A container of messages grouped under a single packet
    pub messages: Vec<UserMessage>,
}

/// A packet crate is essentially a single super packet which packs together multiple user messages and acknowledgments (to the same destination)
///
/// Its main purpose is to batch messages into larger packets (when possible)
pub struct PacketCrateBuilder {
    /// Acknowledgments to pack. Why are we using an option here? To safely work around the borrowchecker
    msg_acknowledgments: Option<(MessageId, MessageAckMap)>,

    /// User messages to pack
    user_messages: Option<Vec<UserMessage>>,

    /// The ID of a packet
    packet_seq_id: Option<PacketSeqId>,

    serbuffer: bitcode::Buffer,

    /// The current size of the packet crate
    size: usize,

    /// The current MTU limit
    mtu: usize,
}

impl PacketCrateBuilder {
    /// The initial size of the packet crate:
    /// - Packet ID
    /// - Base acknowledgment ID (4)
    /// - Acknowledgment map (4)
    /// - Length of user messages (4)
    const INIT_SIZE: usize = size_of::<PacketSeqId>()
        + size_of::<MessageId>()
        + size_of::<MessageAckMap>()
        + size_of::<u32>();

    pub fn new(mtu: usize) -> Self {
        Self {
            msg_acknowledgments: None,
            packet_seq_id: None,
            user_messages: Some(Vec::new()),

            serbuffer: bitcode::Buffer::new(),

            size: Self::INIT_SIZE,
            mtu,
        }
    }

    /// Check if this packet crate can fit the provided amount of bytes
    pub fn can_fit(&self, amount: usize) -> bool {
        (self.size + amount) <= self.mtu
    }

    /// Check how much space is available
    pub fn free_space(&self) -> usize {
        self.mtu - self.size
    }

    pub fn set_packet_id(&mut self, seq_id: PacketSeqId) {
        self.packet_seq_id = Some(seq_id);
    }

    pub fn put_message_acknowledgments(&mut self, base: PacketSeqId, map: PacketAckMap) {
        self.msg_acknowledgments = Some((base, map));
    }

    pub fn put_user_message(&mut self, packet: UserMessage) {
        let size = packet.size();
        assert!(self.can_fit(size), "Unable to fit the provided packet");

        self.user_messages.as_mut().unwrap().push(packet);
        self.size += size;
    }

    /// Clear this packet crate for reusability
    pub fn clear(&mut self) {
        self.msg_acknowledgments = None;
        self.packet_seq_id = None;
        self.user_messages.as_mut().unwrap().clear();

        self.size = Self::INIT_SIZE;
    }

    /// A packet crate packer is empty if it doesn't contain any acknowledgments or user messages
    pub fn is_empty(&self) -> bool {
        self.msg_acknowledgments.is_none() && self.user_messages.as_ref().unwrap().is_empty()
    }

    /// Build this crate and get the slice of the serialized crate packet
    pub fn build(&mut self) -> &[u8] {
        // First of all, create our packet crate

        let (msg_base, msg_map) = self.msg_acknowledgments.unwrap_or((0, 0));
        let seq_id = self.packet_seq_id.expect("No packet ID was supplied");

        let pcrate = PacketCrate {
            seq_id,
            msg_base,
            msg_map,
            messages: self.user_messages.take().unwrap(),
        };

        // Serialize it into bytes
        let serialized = self.serbuffer.encode(&pcrate);

        {
            // Now, clear and put back our user message vector
            let mut user_messages = pcrate.messages;

            user_messages.clear();
            self.user_messages = Some(user_messages);
        }

        // Reset the size of our builder
        self.size = Self::INIT_SIZE;
        self.msg_acknowledgments = None;
        self.packet_seq_id = None;

        // Return the serialized slice
        serialized
    }
}

/// Build an acknowledgment map from the provided sliding ack window
///
/// It will return the base sequence ID from which to acknowledge packets and the map itself.
///
/// The base ID is included in the map
pub fn build_ack_map(window: &SlidingAckWindow) -> (MessageId, MessageAckMap) {
    let base = window.window_position();

    // Initialise the map
    let mut map = 0;

    // The read cursor
    let mut cursor = (PacketAckMap::BITS - 1) << 1;

    // For each bit
    for i in 0..MessageAckMap::BITS {
        // If it's marked - put it on the map as well
        if window.is_marked(base + i) {
            map |= cursor;
        }

        // Shift the cursor to the right
        cursor >>= 1;
    }

    (base, map)
}
