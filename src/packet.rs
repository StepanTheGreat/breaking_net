use bitcode::{Encode, Decode};

/// A kind of a packet with different reliability levels
#[derive(Debug, Encode, Decode)]
enum PacketKind {
    Unreliable = 0,
    UnreliableOrdered = 1,
    ReliableUnordered = 2,
    Reliable = 3
}

/// A packet itself 
#[derive(Debug, Encode, Decode)]
struct Packet {
    seq_id: u32,
    hash: u32,
    kind: PacketKind,
    data: Vec<u8>
} 