use std::io;
use std::net;

use crate::socket::{SimpleSock, SockSettings};
use crate::{MTU_SIZE, MTU_SIZE_PRIVATE, socket_addr};

/// A broadcast listener *listens* for broadcast packets.
///
/// You could use one to for example, listen for game invites and other public information.
pub struct BroadcastListener {
    socket: SimpleSock,
}

impl BroadcastListener {
    /// Create a new broadcast listener that will listen at the provided socket address
    ///
    /// Note that multiple broadcast listeners can sit on the same port, since they shared.
    pub fn new(socket_addr: net::SocketAddr) -> io::Result<Self> {
        let socket = SimpleSock::new_ex(
            socket_addr,
            MTU_SIZE_PRIVATE,
            SockSettings {
                reuses_address: true,
                ..Default::default()
            },
        )?;

        Ok(Self { socket })
    }

    /// Check if this listener has any messages without consuming them from the queue
    pub fn has_messages(&self) -> bool {
        self.socket.has_messages()
    }

    /// Receive a single message from the network.
    ///
    /// [None] means there are no messages
    pub fn recv(&mut self) -> Option<(Vec<u8>, net::SocketAddr)> {
        self.socket
            .recv_from()
            .map(|(data, addr)| (data.to_vec(), addr))
    }
}

/// A socket whose purpose is to specifically write broadcast messages.
///
/// This is a temporary API choice, and in the future will get replaced by allowing sockets to
/// send broadcasts directly
pub struct BroadcastWriter {
    socket: SimpleSock,
    broadcast_addr: net::SocketAddr,
}

impl BroadcastWriter {
    /// Create a new broadcast writer for the provided address and port.
    ///
    /// The port will be used for dispatching broadcasts
    pub fn new(socket_addr: net::SocketAddr, port: u16) -> io::Result<Self> {
        // The address to which we're going to send messages
        let broadcast_addr = socket_addr!(broadcast;port);

        // The capacity is at zero, since we're not going to receive anything
        let socket = SimpleSock::new_ex(
            socket_addr,
            MTU_SIZE_PRIVATE,
            SockSettings {
                broadcaster: true,
                ..Default::default()
            },
        )?;

        Ok(Self {
            socket,
            broadcast_addr,
        })
    }

    /// Update the port of a broadcast writer
    pub fn set_port(&mut self, new_port: u16) {
        self.broadcast_addr = socket_addr!(255,255,255,255;new_port);
    }

    /// Send a broadcast message
    ///
    /// # Panics
    /// This method will panic, if data contains more bytes than is permissible by [MTU_SIZE]
    pub fn send(&mut self, data: &[u8]) -> Result<(), io::Error> {
        assert!(data.len() < MTU_SIZE, "Reached an MTU limit of {MTU_SIZE}");

        self.socket.send_to(data, self.broadcast_addr)
    }
}
