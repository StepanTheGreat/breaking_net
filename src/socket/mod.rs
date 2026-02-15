use rand::seq::SliceRandom;
use socket2 as sock;
use std::{collections::{HashMap, HashSet, VecDeque}, io, mem::MaybeUninit, net, rc::Rc};

use crate::{
    MTU_SIZE, MTU_SIZE_PRIVATE, crc32::{CRC32_SIG_LEN, crc32}, packet::{PacketCrate, PacketCrateBuilder, PacketSeqId, Reliability, UserPacket}, socket::channels::{Channel, ChannelStorage}, window::SlidingAckWindow
};

mod channels;

/// Resend every 2 frames
const RESEND_TIMER: f32 = 1.0/15.0;

/// We'll keep up to 512 packets
const PACKET_WINDOW_BITS: usize = 512;

/// This small module implements utilities for testing different network environments. The main goal is to be able to "reproduce"
/// network instability, to workaround those in tests (because tests are in most cases run locally)
#[cfg(feature = "stress_testing")]
mod stress_testing {
    use std::cell::{Cell, LazyCell, RefCell};

    use rand::{Rng, SeedableRng, rngs::SmallRng};

    thread_local! {
        static PACKET_LOSS_CHANCE: Cell<f32> = Cell::default();
    
        static PACKET_DUBLICATION_CHANCE: Cell<f32> = Cell::default();
    
        static PACKET_CORRUPTION_CHANCE: Cell<f32> = Cell::default();

        static PACKET_REORDER_CHANCE: Cell<f32> = Cell::default();
    
        pub(crate) static RNG_STATE: LazyCell<RefCell<SmallRng>> = LazyCell::new(|| 
            RefCell::new(rand::rngs::SmallRng::from_os_rng())
        );
    }

    fn assert_chance_valid(chance: f32) {
        assert!(
            (0.0..=1.0).contains(&chance), "The chance percentage must be between 0 and 1"
        );
    }

    /// Set the thread-local packet loss chance 
    pub fn set_packet_loss_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        PACKET_LOSS_CHANCE.set(new_chance);
    }

    /// Set the thread-local packet dublication chance 
    pub fn set_packed_dublication_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        PACKET_DUBLICATION_CHANCE.set(new_chance);
    }

    /// Set the thread-local packet dublication chance 
    pub fn set_packed_corruption_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        PACKET_CORRUPTION_CHANCE.set(new_chance);
    }

    /// Set the thread-local packet loss chance 
    pub fn set_packet_reorder_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        PACKET_REORDER_CHANCE.set(new_chance);
    }

    /// Reset the stress-testing environment 
    pub fn reset_stress_environment() {
        set_packed_corruption_chance(0.0);
        set_packed_dublication_chance(0.0);
        set_packet_loss_chance(0.0);
        set_packet_reorder_chance(0.0);
    }


    /// Generate a random number between 0 and 1, and check if it's less than the provided chance (thus returning `true`) 
    fn satisfies_random_chance(chance: f32) -> bool {
        RNG_STATE.with(|rng| 
            rng.borrow_mut().random_range(0.0..=1.0) <= chance
        )
    }

    /// Should this next packet get corrupted?
    pub(crate) fn should_corrupt_packet() -> bool {
        satisfies_random_chance(PACKET_CORRUPTION_CHANCE.get())
    }

    /// Should this next packet get lost?  
    pub(crate) fn should_lose_packet() -> bool {
        satisfies_random_chance(PACKET_LOSS_CHANCE.get())
    }

    /// Should this next packet get dublicated?
    pub(crate) fn should_dublicate_packet() -> bool {
        satisfies_random_chance(PACKET_DUBLICATION_CHANCE.get())
    }

    /// Should the next packets get reordered?
    pub(crate) fn should_reorder_packets() -> bool {
        satisfies_random_chance(PACKET_REORDER_CHANCE.get())
    }
}

#[cfg(feature = "stress_testing")]
pub use stress_testing::*;

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

#[derive(Default, Clone, Copy)]
pub struct SockSettings {
    pub broadcaster: bool,
    pub reuses_address: bool
}

/// A simplified socket structure which directly handles buffers, reading and so on
pub struct SimpleSock {
    /// The socket itself
    socket: sock::Socket,

    addr: net:: SocketAddr,

    /// The receive buffer
    recv_buffer: Box<[u8]>,

    send_buffer: Vec<u8>
}

impl SimpleSock {
    pub fn new_ex(addr: net::SocketAddr, capacity: usize, settings: SockSettings) -> io::Result<Self> {
        
        let domain = if addr.is_ipv4() {
            sock::Domain::IPV4
        } else {
            sock::Domain::IPV6
        };

        // Create a new socket
        let socket = sock::Socket::new(domain, sock::Type::DGRAM, Some(sock::Protocol::UDP))?;

        socket.set_nonblocking(true)?;

        // Apply our options
        socket.set_broadcast(settings.broadcaster)?;
        socket.set_reuse_address(settings.reuses_address)?;

        // Bind it to the provided address
        socket.bind(&addr.into())?;

        let addr = socket.local_addr()
            .expect("The socket is bound")
            .as_socket()
            .unwrap();

        Ok(Self {
            socket,
            addr,
            recv_buffer: vec![0u8; capacity].into_boxed_slice(),
            send_buffer: Vec::with_capacity(capacity)
        })
    }

    pub fn new(addr: net::SocketAddr, capacity: usize) -> io::Result<Self> {
        Self::new_ex(addr, capacity, SockSettings::default())
    }

    /// Send some data to the provided address
    pub fn send_to(&mut self, data: &[u8], to: net::SocketAddr) -> io::Result<()> {
        // If we're stress testing - we'll just not do anything (like if the packet got naturally lost) 
        #[cfg(feature = "stress_testing")]
        if should_lose_packet() {
            return Ok(());
        }

        // Copy the message to our buffer
        self.send_buffer.clear();
        self.send_buffer.extend_from_slice(data);
        
        // Compute the CRC signature
        let crc = crc32(&self.send_buffer);

        // Add it to the end of the message
        self.send_buffer.extend_from_slice(
            &crc.to_be_bytes()
        );

        let data= &self.send_buffer;

        match self.socket.send_to(data, &to.into()) {
            Ok(written) if written == data.len() => Ok(()),
            _ => Err(io::Error::other("Unable to send the packet")),
        }?;

        // If this packet must be dublicated - we'll just send it twice. 
        #[cfg(feature = "stress_testing")]
        if should_dublicate_packet() {

            // For the sake of simplicity we're going to dublicate code here.
            match self.socket.send_to(data, &to.into()) {
                Ok(written) if written == data.len() => Ok(()),
                _ => Err(io::Error::other("Unable to send the packet")),
            }?;
        }

        Ok(())
    }

    /// Receive a packet from anyone
    pub fn recv_from(&mut self) -> Option<(&[u8], net::SocketAddr)> {
        // Casting between MaybeUninit primitive types here is safe
        let buff = unsafe {
            std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(self.recv_buffer.as_mut())
        };

        match self.socket.recv_from(buff) {
            Ok((read, addr)) => {
                // We received less bytes than our CRC signature 
                if read < CRC32_SIG_LEN {
                    return None;
                }

                // If we're stress testing and the packet is supposed to be corrupted - we'll just reverse the received message
                #[cfg(feature = "stress_testing")]
                if should_corrupt_packet() {
                    buff[0..read].reverse();
                }

                // Let's compute the CRC signature from our 
                let crc = crc32(&self.recv_buffer[..read-CRC32_SIG_LEN]);
                let crc_bytes: [u8; CRC32_SIG_LEN] = crc.to_be_bytes();
                
                if &self.recv_buffer[read-CRC32_SIG_LEN..read] != &crc_bytes {
                    return None;
                }

                Some((&self.recv_buffer[..read-CRC32_SIG_LEN], addr.as_socket()?))
            }
            Err(_) => None,
        }
    }

    pub fn addr(&self) -> net::SocketAddr {
        self.addr
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
    /// This packet will be sent only once
    Unreliable(UserPacket),

    /// This packet awaits confirmation from the user and will be resent after some amount of time
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

    /// Update this packet's timer
    fn set_timer(&mut self, new_time: f32) {
        match self {
            Self::Reliable { timer, packet: _ } => {*timer = new_time },
            _ => {}
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

pub struct ReceivedPacket {
    pub data: Vec<u8>, 
    pub reliability: Reliability,
    pub sender: net::SocketAddr
}

struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    /// The amount of packets per second
    packets_per_second: usize,

    /// The maximum amount of packets 
    max_transfer_unit: usize,

    /// The builder with which we'll be building all packets
    crate_builder: PacketCrateBuilder,

    /// Packets to send with their respected decrementing timers
    packet_queue: PacketQueue,

    channels: ChannelStorage,

    /// The sliding window for all reliable packets
    packet_window: SlidingAckWindow,

    /// Sequence IDs of packets that were sent from this connection
    self_acknowledged: HashSet<PacketSeqId>,

    /// Sequence IDs that were received from the other end
    other_acknowledged: HashSet<PacketSeqId>,
}

impl SocketConnection {
    fn new(to: net::SocketAddr) -> Self {
        let packets_per_second = 100;
        let max_transfer_unit = MTU_SIZE_PRIVATE;

        Self {
            to,

            packets_per_second,
            max_transfer_unit,
            crate_builder: PacketCrateBuilder::new(max_transfer_unit),
            packet_queue: PacketQueue::new(),

            packet_window: SlidingAckWindow::new(128),
            channels: ChannelStorage::new(),

            self_acknowledged: HashSet::new(),
            other_acknowledged: HashSet::new(),
        }
    }

    /// Queue a new packet to send through this connection ASAP
    fn queue_packet(&mut self, reliability: Reliability, payload: Vec<u8>) {
        let payload = Rc::new(payload);
        
        // Based on different reliability, we're going to queue them differently
        match reliability {
            // Reliable ordered/unordered get themselves resend timers
            Reliability::Reliable | Reliability::ReliableUnordered => {
                let seq_id = self.packet_queue.reliable_counter.next();

                let packet = if reliability == Reliability::Reliable {
                    UserPacket::Reliable { seq_id, payload }
                } else {
                    UserPacket::ReliableUnordered { seq_id, payload }
                };

                // Insert a new packet that must be dispatched ASAP
                self.packet_queue.queue.push_back(
                    QueuedPacket::Reliable {
                        timer: 0.0, 
                        packet
                    }
                );
            },

            // Unreliable however don't get themselves anything
            Reliability::Unreliable => {
                // Just push a basic unreliable packet
                self.packet_queue.queue.push_back(
                    QueuedPacket::Unreliable(UserPacket::Unreliable { payload })
                );
            }
        }
    }

    /// Acknowledgments have been received on this connection
    fn own_acknowledgments_received(&mut self, acks: &[PacketSeqId]) {
        if acks.is_empty() {
            return;
        }

        self.self_acknowledged.extend(acks);
    }

    fn other_acknowledgment_received(&mut self, ack: PacketSeqId) {
        self.other_acknowledged.insert(ack);
    }
    
    /// A separate polling method that specialises in sending packets
    fn poll_send(&mut self, socket: &mut SimpleSock, dt: f32) {
        let mut candidates = VecDeque::with_capacity(self.packet_queue.queue.len());

        // We're going to go from back to front
        for ind in (0..self.packet_queue.queue.len()).rev() {

            // First we're going to update it
            self.packet_queue.queue[ind].tick(dt);

            // Then clone it
            let packet = self.packet_queue.queue[ind].clone();

            // If the packet is both acknowledged and ready - remove it from the queue
            if !( matches!(packet.sequence_id(), Some(seq_id) if !self.self_acknowledged.contains(&seq_id)) || !packet.is_ready() ) {
                self.packet_queue.queue.remove(ind);
            }

            // And if ready - add to the candidate list
            if packet.is_ready() {
                candidates.push_front(packet);
            }
        }

        #[cfg(feature = "stress_testing")]
        {
            if should_reorder_packets() {
                let (a, b) = candidates.as_mut_slices();
    
                // All this ugly code to essentially simply shuffle this packet queue 
                RNG_STATE.with(|rng| a.shuffle(&mut *rng.borrow_mut()));
                RNG_STATE.with(|rng| b.shuffle(&mut *rng.borrow_mut()));
            }
        }

        let mut cant_fit_stack = Vec::new();

        // How many packets can we even send?
        let mut available_packets = (
            self.packets_per_second as f32 * dt.clamp(0.0, 1.0) // No matter the delta here, we're not going to send more than our PPS in a single second 
        ) as usize;

        // While we have some available packet slots
        while available_packets > 0 {

            // If there are no packets nor acks to send - we'll stop right here
            if candidates.is_empty() && self.other_acknowledged.is_empty() {
                break;
            }

            // While the candidate list is not empty
            while !candidates.is_empty() {

                // Extract the packet
                let packet = candidates.pop_front().unwrap();
    
                // If our crate can fit our packet - put it
                if self.crate_builder.can_fit(packet.size()) {

                    // If our packet is unacknowledged - we're going to reset its timer
                    if let Some(seq_id) = packet.sequence_id() {
                        if self.self_acknowledged.contains(&seq_id) {
                            continue;
                        }

                        // Find it and reset its timer
                        self.packet_queue.queue.iter_mut()
                            .find(|p| matches!(p.sequence_id(), Some(id) if id == seq_id))
                            .unwrap()
                            .set_timer(RESEND_TIMER);
                    }
                    
                    // Consume and push it
                    self.crate_builder.put_user_packet(packet.consume());

                } else {
                    // In any other case - put it in the for-later stack
                    cant_fit_stack.push(packet);
                }

            }

            // Now that we fit all our available packets - let's try to fit some acknowledgments

            // While there are any acknowledgments and our crate can fit some
            while !self.other_acknowledged.is_empty() && self.crate_builder.available_ack_slots() > 0 {
                
                // Take the first one (not in order)
                let seq_id = self.other_acknowledged.iter().next().copied().unwrap();
                
                // Remove it (thus marking it as acknowledged)
                self.other_acknowledged.remove(&seq_id);

                // Then put it into the crate
                self.crate_builder.put_acknowledgments(&[seq_id]);
            }

            // Finally, our crate is ready to go. All we need to do is build and send it
            let data = self.crate_builder.build();
            let _ = socket.send_to(data, self.to);

            // Decrement the amount of packets we got
            available_packets -= 1;

            // Because we'll have some packets that we couldn't fit - we're going to put them back onto the candidate list
            while let Some(packet) = cant_fit_stack.pop() {
                candidates.push_front(packet);
            }
        }

        // If after all this we STILL have packets to send - we're going to send them next frame
        while let Some(packet) = candidates.pop_back() {
            
            // If our packet is un-acknowledged - we're not adding it back on the queue, since it's already there
            if !matches!(packet.sequence_id(), Some(seq_id) if !self.self_acknowledged.contains(&seq_id)) {
                continue;
            }

            self.packet_queue.queue.push_front(packet);
        }

        // Don't forget to clear the acknowledged list of our packets
        self.self_acknowledged.clear();
    }
    
    fn poll(&mut self, socket: &mut SimpleSock, dt: f32) {
        // Then send our own
        self.poll_send(socket, dt);
    }

    /// Process the provided packet (by filtering it out)
    fn process_packet(&mut self, packet: UserPacket) {

        match packet.sequence_id() {
            Some(seq_id) => {
                if self.packet_window.within_bounds(seq_id) {
                    self.channels.process_packet(&self.packet_window, packet);

                    self.packet_window.mark(seq_id);
                }
            },
            None => {
                self.channels.process_packet(&self.packet_window, packet);
            }
        }
    }

    /// Receive all *available* packets
    fn recv_packet(&mut self) -> Option<UserPacket> {
        self.channels.recv_packet(&self.packet_window)
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
    socket: SimpleSock,
    connections: HashMap<net::SocketAddr, SocketConnection>,

    recv_buffer: VecDeque<ReceivedPacket>
}

impl Socket {
    pub fn new(addr: net::SocketAddr) -> io::Result<Self> {
        let socket = SimpleSock::new(addr, MTU_SIZE_PRIVATE)?;

        Ok(Self {
            socket,
            connections: HashMap::with_capacity(2),

            recv_buffer: VecDeque::new()
        })
    }

    /// Is this socket connected to the provided socket? Note that this information might be invalid without polling.
    pub fn is_connected(&self, to: &net::SocketAddr) -> bool {
        self.connections.contains_key(to)
    }

    /// Get this socket's address
    pub fn addr(&self) -> net::SocketAddr {
        self.socket.addr()
    }

    /// Send a packet to the provided address
    pub fn send_to(&mut self, to: &net::SocketAddr, data: &[u8], how: Reliability) {
        assert!(data.len() < MTU_SIZE, "Reached an MTU limit of {MTU_SIZE}");

        self.connections
            .entry(*to)
            .or_insert(SocketConnection::new(*to))
            .queue_packet(how, data.to_owned());
    }

    /// Poll this socket thus updating its inner receive buffer and sending data. 
    pub fn poll(&mut self, dt: f32) {
        assert!(dt >= 0.0);

        // We're going to collect all packets received by this socket
        while let Some((data, sender)) = self.socket.recv_from() {

            // If it's decodable
            if let Ok((acks, packets)) = bitcode::decode::<PacketCrate>(data) {

                // Get or make a new socket connection for this packet
                let mut connection = self.connections.get_mut(&sender);

                // If this is a packet from a known connection - let it mark all the acknowledgments it needs
                if let Some(conn) = connection.as_mut() {
                    conn.own_acknowledgments_received(&acks);
                }

                // Now we're going to iterate every single packet
                for packet in packets {

                    // If we have a connection and our packet contains a sequence ID - we're going to notify our connection about this sequence ID
                    if let Some(conn) = connection.as_mut() {
                        
                        // If this packet has a sequence ID - acknowledge it
                        if let Some(seqid) = packet.sequence_id() {
                            conn.other_acknowledgment_received(seqid);
                        }
                        
                        // And finally - process it (filter, reorder it and so on)
                        conn.process_packet(packet);

                    } else {

                        // In any other case we're just going to buffer it without a connection
                        self.recv_buffer.push_back(ReceivedPacket {
                            sender: sender,

                            // Even though it's factually incorrect - for us there's no connection, 
                            // thus this packet is essentially unreliable no matter what
                            reliability: Reliability::Unreliable,
                            data: packet.consume_payload().unwrap(),
                        });
                    }
                }
            }
        }

        // Now, we're going to poll each connection individually as well
        for (_, connection) in self.connections.iter_mut() {
            connection.poll(&mut self.socket, dt);

            while let Some(packet) = connection.recv_packet() {
                let reliability = packet.reliability();
                let sender = connection.to;

                // Finally, add it to our queue
                self.recv_buffer.push_back(ReceivedPacket { 
                    data: packet.consume_payload().unwrap(), 
                    reliability, 
                    sender 
                });
            }
        }
    }

    /// Check if we got a packet
    pub fn recv_from(&mut self) -> Option<ReceivedPacket> {
        self.recv_buffer.pop_front()
    }

    pub fn has_packets(&self) -> bool {
        !self.recv_buffer.is_empty()
    }

    /// "Establish" a new connection to the provided address.
    /// 
    /// This doesn't actually establish anything, it just creates a logical connection between this address. 
    /// Note that if the other address doesn't send any packets whatsoever - this connection will close very quickly.
    pub fn connect(&mut self, addr: net::SocketAddr) {
        if self.connections.contains_key(&addr) {
            return;
        }

        self.connections.insert(addr, SocketConnection::new(addr));
    }

    /// How many packets are available
    pub fn packets(&self) -> usize {
        self.recv_buffer.len()
    }
}
