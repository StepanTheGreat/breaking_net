use socket2 as sock;
use std::{collections::{HashMap, HashSet, VecDeque}, io, mem::MaybeUninit, net, rc::Rc};

use crate::{
    MTU_SIZE,
    packet::{PacketCrateBuilder, PacketSeqId, Reliability, UserPacket},
};

const MAX_HEARTBEAT: f32 = 5.0;

/// A super simple sequence counter, that just increments and wrap arounds sequence ids
struct SequenceCounter(PacketSeqId);
impl SequenceCounter {
    fn new(start: PacketSeqId) -> Self {
        Self(start)
    }

    /// Cycle the next value
    fn next(&mut self) -> PacketSeqId {
        let next = self.0;
        self.0 = self.0.wrapping_add(1);

        next
    }
}

/// A simplified socket structure which directly handles buffers, reading and so on
pub struct SimpleSock {
    /// The socket itself
    socket: sock::Socket,

    /// The receive buffer
    buffer: Box<[u8]>,
}

impl SimpleSock {
    pub fn new(socket: sock::Socket, capacity: usize) -> Self {
        Self {
            socket,
            buffer: vec![0u8; capacity].into_boxed_slice(),
        }
    }

    /// Get a read reference to the underlying socket
    pub fn socket(&self) -> &sock::Socket {
        &self.socket
    }

    /// Send some data to the provided address
    pub fn send_to(&mut self, data: &[u8], to: net::SocketAddr) -> io::Result<()> {
        match self.socket.send_to(data, &to.into()) {
            Ok(written) if written == data.len() => Ok(()),
            _ => Err(io::Error::other("Unable to send the packet")),
        }
    }

    /// Receive a packet from anyone
    pub fn recv_from(&mut self) -> Option<(&[u8], net::SocketAddr)> {
        // Casting between MaybeUninit primitive types here is safe
        let buff = unsafe {
            std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(self.buffer.as_mut())
        };

        match self.socket.recv_from(buff) {
            Ok((read, addr)) => {
                // Nothing to do
                if read == 0 {
                    return None;
                }

                Some((&self.buffer[0..read], addr.as_socket()?))
            }
            Err(_) => None,
        }
    }

    /// Does this socket have any packets?
    ///
    /// Calling this method, compared to [SimpleSock::recv_from], doesn't consume the packets
    pub fn has_packets(&self) -> bool {
        self.socket.peek_sender().is_ok()
    }
}

#[derive(Clone)]
enum QueuedPacket {
    Unreliable(UserPacket),
    Reliable {
        timer: f32,
        packet: UserPacket
    }
}

impl QueuedPacket {
    fn tick(&mut self, dt: f32) {
        match self {
            Self::Unreliable(_) => (), 
            Self::Reliable { timer, packet: _ } => {
                *timer = (*timer - dt).max(0.0);
            }
        }
    }

    /// Is this queued packet ready?
    fn is_ready(&self) -> bool {
        match self {
            // Unreliable packets are always ready
            Self::Unreliable(_) => true,

            // Reliable packets however, are not
            Self::Reliable { timer, packet: _ } => *timer == 0.0
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Reliable { timer: _, packet } => packet.size(),
            Self::Unreliable(packet) => packet.size()
        }
    }

    fn sequence_id(&self) -> Option<PacketSeqId> {
        match self {
            Self::Reliable { timer: _, packet } => packet.sequence_id(),
            Self::Unreliable(packet) => packet.sequence_id()
        }
    }

    fn consume(self) -> UserPacket {
        match self {
            Self::Reliable { timer: _, packet } => packet,
            Self::Unreliable(packet) => packet
        }
    }
}

struct PacketQueue {
    /// A queue of packets
    queue: VecDeque<QueuedPacket>,

    /// The counter to obtain sequence IDs from
    reliable_counter: SequenceCounter,
}

impl PacketQueue {
    const INIT_PACKET_CAPACITY: usize = 20;

    fn new() -> Self {
        Self { 
            queue: VecDeque::with_capacity(Self::INIT_PACKET_CAPACITY), 

            reliable_counter: SequenceCounter::new(0),
        }
    }
}

struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    /// The amount of packets per second
    packets_per_second: usize,

    /// The maximum amount of packets 
    max_transfer_unit: usize,

    /// How much time has passed since the last heartbeat? This must be reset whenever we receive either an explicit
    last_heartbeat: f32,

    /// The builder with which we'll be building all packets
    crate_builder: PacketCrateBuilder,

    /// Packets to send with their respected decrementing timers
    packet_queue: PacketQueue,

    acknowledged: HashSet<PacketSeqId>
}

impl SocketConnection {
    fn new(to: net::SocketAddr) -> Self {
        let packets_per_second = 100;
        let max_transfer_unit = MTU_SIZE;

        Self {
            to,
            last_heartbeat: MAX_HEARTBEAT,

            packets_per_second,
            max_transfer_unit,
            crate_builder: PacketCrateBuilder::new(max_transfer_unit),

            packet_queue: PacketQueue::new(),

            acknowledged: HashSet::with_capacity(10)
        }
    }

    /// Queue a new packet to send through this connection ASAP
    fn queue_packet(&mut self, reliability: Reliability, payload: Vec<u8>) {
        let payload = Rc::new(payload);
        
        match reliability {
            Reliability::Reliable => {
                let seq_id = self.packet_queue.reliable_counter.next();

                // Insert a new packet that must be dispatched ASAP
                self.packet_queue.queue.push_back(
                    QueuedPacket::Reliable {
                        timer: 0.0, 
                        packet: UserPacket::Reliable { seq_id, payload }
                    }
                );
            },
            Reliability::Unreliable => {
                // Just push a basic unreliable packet
                self.packet_queue.queue.push_back(
                    QueuedPacket::Unreliable(UserPacket::Unreliable { payload })
                );
            }
        }
    }

    /// Acknowledgments have been received on this connection
    fn acknowledgments_received(&mut self, acks: &[PacketSeqId]) {
        if acks.is_empty() {
            return;
        }

        // For each acknowledged ID
        for ack in acks.iter().copied() {
            // Add it to our acknowledged list
            self.acknowledged.insert(ack);
        }
    }

    fn poll(&mut self, socket: &mut SimpleSock, dt: f32) {
        todo!();

        let mut candidates = Vec::with_capacity(self.packet_queue.queue.len());

        // First we're going to remove all packets that have been acknowledged
        self.packet_queue.queue.retain_mut(|packet| {
            !matches!(packet.sequence_id(), Some(seq_id) if self.acknowledged.contains(&seq_id))
        });
        
        // For each packet we're going to simply update them and if ready put in the candidate list
        for queued_packet in self.packet_queue.queue.iter_mut() {
            queued_packet.tick(dt);

            if queued_packet.is_ready() {
                candidates.push(queued_packet.clone());
            }
        }

        // How many packets can we even send?
        let mut available_packets = (
            self.packets_per_second as f32 * dt.clamp(0.0, 1.0) // No matter the delta here, we're not going to send more than our PPS in a single second 
        ) as usize;

        // TODO: Build packets and send them

        // Don't forget to clear the acknowledged list
        self.acknowledged.clear();
    }
}

pub enum SocketEvent {
    Connection(net::SocketAddr),
    Disconnection(net::SocketAddr),
    Packet {
        from: net::SocketAddr,
        payload: Box<[u8]>,
    },
}

pub struct Socket {
    socket: sock::Socket,
    addr: net::SocketAddr,
    connections: HashMap<net::SocketAddr, SocketConnection>,
}

impl Socket {
    pub fn new(addr: net::SocketAddr) -> io::Result<Self> {
        let domain = if addr.is_ipv4() {
            sock::Domain::IPV4
        } else {
            sock::Domain::IPV6
        };

        let socket = sock::Socket::new(domain, sock::Type::DGRAM, Some(sock::Protocol::UDP))?;

        Ok(Self {
            socket,
            addr,
            connections: HashMap::with_capacity(2),
        })
    }

    /// Is this socket connected to the provided socket? Note that this information might be invalid without polling.
    pub fn is_connected(&self, to: &net::SocketAddr) -> bool {
        self.connections.contains_key(to)
    }

    /// Send a packet to the provided address
    pub fn send_to(&mut self, to: &net::SocketAddr, data: &[u8], how: Reliability) {
        self.connections.get_mut(to).unwrap()
            .queue_packet(how, data.to_owned());
    }
}
