use std::rc::Rc;
use bitcode::{Decode, Encode};

/// An ID of a packet
pub type PacketSeqId = u32;

/// An ID of a uniquely identifiable message. The difference between the packet, is that a packet is only sent once. A message however, can be
/// resent until it's actually received. This is an important separation when measuring RTT and other metrics.
pub type MessageId = u32;

/// The message data itself
pub type MessagePayload = Rc<Vec<u8>>;

pub type PacketAckMap = u32;

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
    ReliableUnordered {
        msg_id: MessageId
    },
    Reliable {
        msg_id: MessageId
    }
}

#[derive(Clone, Encode, Decode)]
pub struct UserMessage {
    kind: UserMessageKind,
    payload: MessagePayload,
}

impl UserMessage {
    fn new(kind: UserMessageKind, payload: MessagePayload) -> Self {
        Self {
            kind, 
            payload
        }
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
            UserMessageKind::Unreliable => 0
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
/// 
/// It includes:
/// - An acknowledgment base
/// - An acknowledgment map
/// - A list of messages
#[derive(Encode, Decode)]
pub struct PacketCrate {
    pub seq_id: PacketSeqId,
    pub ack_base: PacketSeqId,
    pub ack_map: PacketAckMap,
    pub messages: Vec<UserMessage>
}

/// A packet crate is essentially a single super packet which packs together multiple user messages and acknowledgments (to the same destination)
///
/// Its main purpose is to batch messages into larger packets (when possible)
pub struct PacketCrateBuilder {
    /// Acknowledgments to pack. Why are we using an option here? To safely work around the borrowchecker
    acknowledgments: Option<(PacketSeqId, PacketAckMap)>,

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
    const INIT_SIZE: usize = size_of::<PacketSeqId>() + size_of::<PacketSeqId>() + size_of::<PacketAckMap>() + size_of::<u32>();

    pub fn new(mtu: usize) -> Self {
        Self {
            acknowledgments: None,
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

    pub fn put_acknowledgments(&mut self, base: PacketSeqId, map: PacketAckMap) {
        self.acknowledgments = Some((base, map));
    }

    pub fn put_user_message(&mut self, packet: UserMessage) {
        let size = packet.size();
        assert!(self.can_fit(size));

        self.user_messages.as_mut().unwrap().push(packet);
        self.size += size;
    }

    /// How many acknowledgments can this crate fit?
    pub fn available_ack_slots(&self) -> usize {
        self.free_space() / size_of::<PacketSeqId>()
    }

    /// Clear this packet crate for reusability
    pub fn clear(&mut self) {
        self.acknowledgments = None;
        self.packet_seq_id = None;
        self.user_messages.as_mut().unwrap().clear();

        self.size = Self::INIT_SIZE;
    }

    /// A packet crate packer is empty if it doesn't contain any acknowledgments or user messages
    pub fn is_empty(&self) -> bool {
        self.acknowledgments.is_none() && self.user_messages.as_ref().unwrap().is_empty()
    }

    /// Build this crate and get the slice of the serialized crate packet
    pub fn build(&mut self) -> &[u8] {
        // First of all, create our packet crate

        let (ack_base, ack_map) = self.acknowledgments.unwrap_or((0, 0));
        let seq_id = self.packet_seq_id.expect("No packet ID was supplied");

        let pcrate = PacketCrate {
            seq_id,
            ack_base,
            ack_map,
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
        self.acknowledgments = None;
        self.packet_seq_id = None;

        // Return the serialized slice
        serialized
    }
}


/// Build an acknowledgment map from the provided **sorted** acknowledgment slice
/// 
/// It will return the base sequence ID from which to acknowledge packets and the map itself. 
/// 
/// This will not include base sequence ID into the map
pub fn build_ack_map(acks: &[PacketSeqId]) -> (PacketSeqId, PacketAckMap) {
    assert!(acks.is_sorted());

    // The accepted default
    if acks.is_empty() {
        return (0, 0)
    }
    
    // Initialise the map
    let mut map = 0;

    // Get the base
    let base = acks[0];

    for ack in acks.iter().copied() {
        
        // Compute the delta (binary index)
        let bind = ack-base;

        // We'll stop here
        if bind >= PacketAckMap::BITS {
            break;
        }

        // Finally, insert it into the map
        map |= 1 << ((PacketAckMap::BITS-1)-bind);
    } 

    (base, map)
}