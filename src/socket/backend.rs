use socket2 as sock;

use std::{any::Any, io, mem::MaybeUninit, net, time::Duration};

use crate::{
    PROTOCOL_SIGNATURE, SocketOptions,
    crc32::CRC32,
};

/// A socket backend that can be used in conjunction with the high-level socket.
///
/// The primary purpose of this abstraction is to allow virtual sockets that interact within their own virtual network
/// (for testing and batching purposes). This however can easily be extended to other use cases.
pub trait SocketBackend: Any {
    /// Send some data to the provided address
    fn send_to(&mut self, data: &[u8], to: net::SocketAddr) -> io::Result<()>;

    /// Receive a message from anyone
    fn recv_from(&mut self) -> Option<(&[u8], net::SocketAddr)>;

    /// Gets called on every single poll. Useful for internal mechanisms, timings and so on
    fn poll(&mut self, dt: Duration);

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

    /// The receive buffer
    recv_buffer: Box<[u8]>,

    crc: CRC32,
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
        
        let crc = CRC32::new(mtu, *PROTOCOL_SIGNATURE);

        Ok(Self {
            socket,
            addr,
            crc,
            recv_buffer: vec![0u8; mtu].into_boxed_slice(),
        })
    }

    pub fn new(addr: net::SocketAddr, capacity: usize) -> io::Result<Self> {
        Self::new_ex(addr, capacity, &SocketOptions::default())
    }
}

impl SocketBackend for SocketUDP {
    fn send_to(&mut self, data: &[u8], to: net::SocketAddr) -> io::Result<()> {

        let signed_data = match self.crc.sign(data) {
            Some(data) => data,
            None => return Err(io::Error::other("Reached socket's MTU limits"))
        };

        match self.socket.send_to(signed_data, &to.into()) {
            Ok(written) if written == signed_data.len() => Ok(()),
            _ => Err(io::Error::other("Unable to send the message")),
        }?;

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
                if let Some(data) = self.crc.validate(&self.recv_buffer[..read]) {
                    Some((data, addr.as_socket()?))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    fn poll(&mut self, _: Duration) {}

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