use socket2 as sock;

/// This small module implements utilities for testing different network environments. The main goal is to be able to "reproduce"
/// network instability, to workaround those in tests (because tests are in most cases run locally)
#[cfg(feature = "stress_testing")]
mod stress_testing {
    use std::cell::{Cell, LazyCell, RefCell};

    use rand::{Rng, SeedableRng, rngs::SmallRng};

    thread_local! {
        static MESSAGE_LOSS_CHANCE: Cell<f32> = Cell::default();

        static MESSAGE_DUBLICATION_CHANCE: Cell<f32> = Cell::default();

        static MESSAGE_CORRUPTION_CHANCE: Cell<f32> = Cell::default();

        static MESSAGE_REORDER_CHANCE: Cell<f32> = Cell::default();

        pub(crate) static RNG_STATE: LazyCell<RefCell<SmallRng>> = LazyCell::new(||
            RefCell::new(SmallRng::from_os_rng())
        );
    }

    fn assert_chance_valid(chance: f32) {
        assert!(
            (0.0..=1.0).contains(&chance),
            "The chance percentage must be between 0 and 1"
        );
    }

    /// Set the thread-local message loss chance
    pub fn set_message_loss_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        MESSAGE_LOSS_CHANCE.set(new_chance);
    }

    /// Set the thread-local message dublication chance
    pub fn set_message_dublication_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        MESSAGE_DUBLICATION_CHANCE.set(new_chance);
    }

    /// Set the thread-local message dublication chance
    pub fn set_message_corruption_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        MESSAGE_CORRUPTION_CHANCE.set(new_chance);
    }

    /// Set the thread-local message loss chance
    pub fn set_message_reorder_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        MESSAGE_REORDER_CHANCE.set(new_chance);
    }

    /// Reset the stress-testing environment
    pub fn reset_stress_environment() {
        set_message_corruption_chance(0.0);
        set_message_dublication_chance(0.0);
        set_message_loss_chance(0.0);
        set_message_reorder_chance(0.0);
    }

    /// Generate a random number between 0 and 1, and check if it's less than the provided chance (thus returning `true`)
    fn satisfies_random_chance(chance: f32) -> bool {
        RNG_STATE.with(|rng| rng.borrow_mut().random_range(0.0..=1.0) <= chance)
    }

    /// Should this next message get corrupted?
    pub(crate) fn should_corrupt_message() -> bool {
        satisfies_random_chance(MESSAGE_CORRUPTION_CHANCE.get())
    }

    /// Should this next message get lost?  
    pub(crate) fn should_lose_message() -> bool {
        satisfies_random_chance(MESSAGE_LOSS_CHANCE.get())
    }

    /// Should this next message get dublicated?
    pub(crate) fn should_dublicate_message() -> bool {
        satisfies_random_chance(MESSAGE_DUBLICATION_CHANCE.get())
    }

    /// Should the next messages get reordered?
    pub(crate) fn should_reorder_messages() -> bool {
        satisfies_random_chance(MESSAGE_REORDER_CHANCE.get())
    }
}

use std::{io, mem::MaybeUninit, net};

#[cfg(feature = "stress_testing")]
pub use stress_testing::*;

use crate::{
    PROTOCOL_SIGNATURE, SocketOptions,
    crc32::{CRC32_SIG_LEN, crc32_sign, crc32_verify},
};

/// A socket backend that can be used in conjunction with the high-level socket.
///
/// The primary purpose of this abstraction is to allow virtual sockets that interact within their own virtual network
/// (for testing and batching purposes). This however can easily be extended to other use cases.
pub trait SocketBackend {
    /// Send some data to the provided address
    fn send_to(&mut self, data: &[u8], to: net::SocketAddr) -> io::Result<()>;

    /// Receive a message from anyone
    fn recv_from(&mut self) -> Option<(&[u8], net::SocketAddr)>;

    /// Get this socket's bound address
    fn addr(&self) -> net::SocketAddr;

    /// Does this socket have any messages?
    ///
    /// Calling this method, compared to [SimpleSock::recv_from], doesn't consume the messages
    fn has_messages(&self) -> bool;
}

/// A simplified socket structure which directly handles buffers, reading and so on
pub struct SocketUDP {
    /// The socket itself
    socket: sock::Socket,

    addr: net::SocketAddr,

    /// Protocol's signature
    signature: &'static str,

    /// The receive buffer
    recv_buffer: Box<[u8]>,

    send_buffer: Box<[u8]>,

    mtu: usize,
}

impl SocketUDP {
    pub fn new_ex(addr: net::SocketAddr, mtu: usize, options: &SocketOptions) -> io::Result<Self> {
        let domain = if addr.is_ipv4() {
            sock::Domain::IPV4
        } else {
            sock::Domain::IPV6
        };

        // Create a new socket
        let socket = sock::Socket::new(domain, sock::Type::DGRAM, Some(sock::Protocol::UDP))?;

        socket.set_nonblocking(true)?;

        // Apply our options
        socket.set_broadcast(options.broadcaster)?;
        socket.set_reuse_address(options.reuses_address)?;

        // Bind it to the provided address
        socket.bind(&addr.into())?;

        let addr = socket
            .local_addr()
            .expect("The socket is bound")
            .as_socket()
            .unwrap();

        // Get our protocol signature
        let signature = *PROTOCOL_SIGNATURE;

        // Our buffers will be *slightly* larger to accomodate for the signature. The signature however isn't send,
        // it's only used for CRC checks
        let buffer_capacity = mtu + signature.len();

        Ok(Self {
            socket,
            addr,
            signature,
            mtu,
            recv_buffer: vec![0u8; buffer_capacity].into_boxed_slice(),
            send_buffer: vec![0u8; buffer_capacity].into_boxed_slice(),
        })
    }

    pub fn new(addr: net::SocketAddr, capacity: usize) -> io::Result<Self> {
        Self::new_ex(addr, capacity, &SocketOptions::default())
    }
}

impl SocketBackend for SocketUDP {
    fn send_to(&mut self, data: &[u8], to: net::SocketAddr) -> io::Result<()> {
        // If we're stress testing - we'll just not do anything (like if the message got naturally lost)
        #[cfg(feature = "stress_testing")]
        if should_lose_message() {
            return Ok(());
        }

        if data.len() > self.mtu - CRC32_SIG_LEN {
            return Err(io::Error::other("Reached socket's MTU limits"));
        }

        // TODO: The socket shouldn't be responsible for verifying data integrity. It should be the responsibility of the layer
        // TODO: above. A socket is just a dumb primitive for sending/receiving data (and simulating network environment)
        let data_len = data.len();
        let data_crc_len = data_len + CRC32_SIG_LEN;

        // Copy the message to our buffer
        self.send_buffer[..data_len].copy_from_slice(data);

        // Sign it
        crc32_sign(&mut self.send_buffer[..data_crc_len], Some(self.signature));

        // Augment our data slice to account for our new signature
        let data = &self.send_buffer[..data_crc_len];

        match self.socket.send_to(data, &to.into()) {
            Ok(written) if written == data.len() => Ok(()),
            _ => Err(io::Error::other("Unable to send the message")),
        }?;

        // If this message must be dublicated - we'll just send it twice.
        #[cfg(feature = "stress_testing")]
        if should_dublicate_message() {
            // For the sake of simplicity we're going to dublicate code here.
            match self.socket.send_to(data, &to.into()) {
                Ok(written) if written == data.len() => Ok(()),
                _ => Err(io::Error::other("Unable to send the message")),
            }?;
        }

        Ok(())
    }

    /// Receive a message from anyone
    fn recv_from(&mut self) -> Option<(&[u8], net::SocketAddr)> {
        let socket_read = {
            // Casting between &mut [u8] and &mut MaybeUninit<u8> here is safe. This mutable buffer reference is only valid within this single method call
            let recv_buff =
                unsafe { &mut *(self.recv_buffer.as_mut() as *mut [u8] as *mut [MaybeUninit<u8>]) };
            self.socket.recv_from(recv_buff)
        };

        match socket_read {
            Ok((read, addr)) => {
                // We received less bytes than our CRC signature
                if read < CRC32_SIG_LEN {
                    return None;
                }

                // If we're stress testing and the message is supposed to be corrupted - we'll just reverse the received message
                #[cfg(feature = "stress_testing")]
                if should_corrupt_message() {
                    self.recv_buffer[0..read].reverse();
                }

                // Signature mismatch, early return
                if !crc32_verify(&self.recv_buffer[..read], Some(self.signature)) {
                    return None;
                }

                // Read everything excluding the signature
                let data_len = read - CRC32_SIG_LEN;
                Some((&self.recv_buffer[..data_len], addr.as_socket()?))
            }
            Err(_) => None,
        }
    }

    fn addr(&self) -> net::SocketAddr {
        self.addr
    }

    /// Does this socket have any messages?
    ///
    /// Calling this method, compared to [SimpleSock::recv_from], doesn't consume the messages
    fn has_messages(&self) -> bool {
        self.socket.peek_sender().is_ok()
    }
}
