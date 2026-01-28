//! We're using a component-based approach, where packets are constructed using different components. The primary idea is that packets can arrive with
//! different data

use bitcode::{Decode, Encode};

/// An ID of a packet (present on reliable and unreliable-ordered channels)
pub type PacketSeqId = u32;

/// The checksum of the packet present everywhere (allows verirying if a packet isn't corrupted)
pub type PacketChecksum = u32;

/// The packet data itself
pub type PacketPayload = Vec<u8>;

#[derive(Encode, Decode)]
pub enum UserPacket {
    Unreliable {
        payload: PacketPayload,
    },
    Reliable {
        seq_id: PacketSeqId,
        payload: PacketPayload,
    },
}

impl UserPacket {
    pub fn is_reliable(&self) -> bool {
        match self {
            Self::Reliable {
                seq_id: _,
                payload: _,
            } => true,
            Self::Unreliable { payload: _ } => false,
        }
    }

    /// A conservative estimate of the total packet size
    pub fn size(&self) -> usize {
        match self {
            Self::Reliable { seq_id: _, payload } => {
                // Sequence ID + Payload length + payload itself
                size_of::<PacketSeqId>() + size_of::<u32>() + payload.len()
            }
            Self::Unreliable { payload } => {
                // Payload length + payload itself
                size_of::<u32>() + payload.len()
            }
        }
    }

    /// Get a sequence id of this packet, if present
    pub fn sequence_id(&self) -> Option<PacketSeqId> {
        match self {
            Self::Reliable { seq_id, payload: _ } => Some(*seq_id),
            Self::Unreliable { payload: _ } => None,
        }
    }
}

/// A packet crate is essentially a single super packet which packs together multiple user packets and acknowledgments (to the same destination)
///
/// Its main purpose is to batch packets into larger packets (when possible)
pub struct PacketCrateBuilder {
    /// Acknowledgments to pack. Why are we using an option here? To safely work around the borrowchecker
    acknowledgments: Option<Vec<u32>>,

    /// User packets to pack
    user_packets: Option<Vec<UserPacket>>,

    serbuffer: bitcode::Buffer,

    /// The current size of the packet crate
    size: usize,

    /// The current MTU limit
    mtu: usize,
}

/// The inherent serialisation type behind packet crate
pub type PacketCrate = (Vec<u32>, Vec<UserPacket>);

impl PacketCrateBuilder {
    /// The initial size of the packet crate
    const INIT_SIZE: usize = size_of::<u32>() * 2;

    pub fn new(mtu: usize) -> Self {
        Self {
            acknowledgments: Some(Vec::new()),
            user_packets: Some(Vec::new()),

            serbuffer: bitcode::Buffer::new(),

            size: Self::INIT_SIZE,
            mtu,
        }
    }

    /// Check if this packet crate can fit the provided amount
    pub fn can_fit(&self, amount: usize) -> bool {
        (self.size + amount) <= self.mtu
    }

    /// Check how much space is available
    pub fn free_space(&self) -> usize {
        self.mtu - self.size
    }

    pub fn put_acknowledgments(&mut self, acks: &[u32]) {
        // Make sure that we can actually fit these acknowledgments
        let size = size_of_val(acks);
        assert!(self.can_fit(size));

        // Add them to our vector
        self.acknowledgments
            .as_mut()
            .unwrap()
            .extend_from_slice(acks);

        // Increment the size of the packer
        self.size += size;
    }

    pub fn put_user_packet(&mut self, packet: UserPacket) {
        let size = packet.size();
        assert!(self.can_fit(size));

        self.user_packets.as_mut().unwrap().push(packet);
        self.size += size;
    }

    /// Clear this packet crate for reusability
    pub fn clear(&mut self) {
        self.acknowledgments.as_mut().unwrap().clear();
        self.user_packets.as_mut().unwrap().clear();

        self.size = Self::INIT_SIZE;
    }

    /// A packet crate packer is empty if it doesn't contain any acknowledgments or user packets
    pub fn is_empty(&self) -> bool {
        self.acknowledgments.as_ref().unwrap().is_empty()
            && self.user_packets.as_ref().unwrap().is_empty()
    }

    /// Build this crate and get the slice of the serialized crate packet
    pub fn build(&mut self) -> &[u8] {
        // First of all, create our packet crate
        let pcrate: PacketCrate = (
            self.acknowledgments.take().unwrap(),
            self.user_packets.take().unwrap(),
        );

        // Serialize it into bytes
        let serialized = self.serbuffer.encode(&pcrate);

        {
            // Now, put back its contents and clear them
            let (mut acknowledgments, mut user_packets) = pcrate;

            acknowledgments.clear();
            user_packets.clear();

            self.acknowledgments = Some(acknowledgments);
            self.user_packets = Some(user_packets);
        }

        // Reset the size of our builder
        self.size = Self::INIT_SIZE;

        // Return the serialized slice
        serialized
    }
}
