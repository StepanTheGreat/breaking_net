//! We're using a component-based approach, where packets are constructed using different components. The primary idea is that packets can arrive with
//! different data

use std::rc::Rc;

use bitcode::{Decode, Encode};

/// An ID of a packet (present on reliable and unreliable-ordered channels)
pub type PacketSeqId = u32;

/// The checksum of the packet present everywhere (allows verirying if a packet isn't corrupted)
pub type PacketChecksum = [u8; 4];

/// The packet data itself
pub type PacketPayload = Rc<Vec<u8>>;

pub type PacketAckMap = u32;

/// Different kinds of reliability
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reliability {
    /// A packet is fully unreliable
    Unreliable,

    /// Packets are resent, but they arrive in undefined order
    ReliableUnordered,

    /// Packets arrive and get processed in the same order they were sent
    Reliable,
}

#[derive(Clone, Encode, Decode)]
pub enum UserPacketKind {
    Unreliable,
    ReliableUnordered {
        seq_id: PacketSeqId
    },
    Reliable {
        seq_id: PacketSeqId
    }
}

#[derive(Clone, Encode, Decode)]
pub struct UserPacket {
    kind: UserPacketKind,
    payload: PacketPayload,
}

impl UserPacket {
    fn new(kind: UserPacketKind, payload: PacketPayload) -> Self {
        Self {
            kind, 
            payload
        }
    }

    pub fn new_reliable(seq_id: PacketSeqId, payload: PacketPayload) -> Self {
        Self::new(UserPacketKind::Reliable { seq_id }, payload)
    }

    pub fn new_reliable_unordered(seq_id: PacketSeqId, payload: PacketPayload) -> Self {
        Self::new(UserPacketKind::ReliableUnordered { seq_id }, payload)
    }

    pub fn new_unreliable(payload: PacketPayload) -> Self {
        Self::new(UserPacketKind::Unreliable, payload)
    }

    pub fn is_reliable(&self) -> bool {
        match self.kind {
            UserPacketKind::Reliable { .. } => true,
            UserPacketKind::ReliableUnordered { .. } => true,
            UserPacketKind::Unreliable => false,
        }
    }

    /// A conservative estimate of the total packet size
    pub fn size(&self) -> usize {
        // The cost of the payload (length + data)
        let payload_size = size_of::<u32>() + self.payload.len();

        // The cost of the sequence ID in reliable packets
        let seq_id_size = match self.kind {
            UserPacketKind::ReliableUnordered { .. } => size_of::<PacketSeqId>(),
            UserPacketKind::Reliable { .. } => size_of::<PacketSeqId>(),
            UserPacketKind::Unreliable => 0
        };

        // The cost of the enum tag for our packet
        let tag_size = 1;

        tag_size + payload_size + seq_id_size
    }

    /// Get a sequence id of this packet, if present
    pub fn sequence_id(&self) -> Option<PacketSeqId> {
        match self.kind {
            UserPacketKind::Reliable { seq_id, .. } => Some(seq_id),
            UserPacketKind::ReliableUnordered { seq_id, .. } => Some(seq_id),
            UserPacketKind::Unreliable => None,
        }
    }

    /// Get this packet's reliability value
    pub fn reliability(&self) -> Reliability {
        match self.kind {
            UserPacketKind::Reliable { .. } => Reliability::Reliable,
            UserPacketKind::ReliableUnordered { .. } => Reliability::ReliableUnordered,
            UserPacketKind::Unreliable => Reliability::Unreliable,
        }
    }

    /// Consume this packet's payload. This will return [None] if it still has active references
    pub fn consume_payload(self) -> Option<Vec<u8>> {
        Rc::into_inner(self.payload)
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

/// A packet crate is essentially a single super packet which packs together multiple user packets and acknowledgments (to the same destination)
///
/// Its main purpose is to batch packets into larger packets (when possible)
pub struct PacketCrateBuilder {
    /// Acknowledgments to pack. Why are we using an option here? To safely work around the borrowchecker
    acknowledgments: Option<(PacketSeqId, PacketAckMap)>,

    /// User packets to pack
    user_packets: Option<Vec<UserPacket>>,

    serbuffer: bitcode::Buffer,

    /// The current size of the packet crate
    size: usize,

    /// The current MTU limit
    mtu: usize,
}

/// The inherent serialisation type behind packet crate
/// 
/// It includes:
/// - An acknowledgment base
/// - An acknowledgment map
/// - A list of packets
pub type PacketCrate = (PacketSeqId, PacketAckMap, Vec<UserPacket>);

impl PacketCrateBuilder {
    /// The initial size of the packet crate:
    /// - Base acknowledgment ID (4)
    /// - Acknowledgment map (4)
    /// - Length of user packets (4)
    const INIT_SIZE: usize = size_of::<PacketSeqId>() + size_of::<PacketAckMap>() + size_of::<u32>();

    pub fn new(mtu: usize) -> Self {
        Self {
            acknowledgments: None,
            user_packets: Some(Vec::new()),

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

    pub fn put_acknowledgments(&mut self, base: PacketSeqId, map: PacketAckMap) {
        self.acknowledgments = Some((base, map));
    }

    pub fn put_user_packet(&mut self, packet: UserPacket) {
        let size = packet.size();
        assert!(self.can_fit(size));

        self.user_packets.as_mut().unwrap().push(packet);
        self.size += size;
    }

    /// How many acknowledgments can this crate fit?
    pub fn available_ack_slots(&self) -> usize {
        self.free_space() / size_of::<PacketSeqId>()
    }

    /// Clear this packet crate for reusability
    pub fn clear(&mut self) {
        self.acknowledgments = None;
        self.user_packets.as_mut().unwrap().clear();

        self.size = Self::INIT_SIZE;
    }

    /// A packet crate packer is empty if it doesn't contain any acknowledgments or user packets
    pub fn is_empty(&self) -> bool {
        self.acknowledgments.is_none() && self.user_packets.as_ref().unwrap().is_empty()
    }

    /// Build this crate and get the slice of the serialized crate packet
    pub fn build(&mut self) -> &[u8] {
        // First of all, create our packet crate

        let (ack_base, ack_map) = self.acknowledgments.unwrap_or((0, 0));
        let pcrate: PacketCrate = (
            ack_base,
            ack_map,
            self.user_packets.take().unwrap(),
        );

        // Serialize it into bytes
        let serialized = self.serbuffer.encode(&pcrate);

        {
            // Now, clear and put back our user packet vector
            let (_, _, mut user_packets) = pcrate;

            user_packets.clear();
            self.user_packets = Some(user_packets);
        }

        // Reset the size of our builder
        self.size = Self::INIT_SIZE;
        self.acknowledgments = None;

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