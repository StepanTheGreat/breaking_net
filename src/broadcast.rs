use std::fmt::Debug;
use std::io;
use std::net;

use crate::MTU_SIZE_PRIVATE;
use crate::SocketOptions;
use crate::socket::{SocketBackend, SocketUDP};

/// A broadcast listener *listens* for broadcast packets.
///
/// You could use one to for example, listen for game invites and other public information.
pub struct BroadcastListener {
    listen_addr: net::SocketAddr,
    socket: SocketUDP,
}

impl BroadcastListener {
    /// Create a new broadcast listener that will listen at the provided socket address
    ///
    /// Note that multiple broadcast listeners can sit on the same port, since they shared.
    pub fn new(listen_addr: net::SocketAddr) -> io::Result<Self> {
        let socket = SocketUDP::new_ex(
            listen_addr,
            MTU_SIZE_PRIVATE,
            &SocketOptions {
                reuses_address: true,
                ..Default::default()
            },
        )?;

        Ok(Self {
            listen_addr,
            socket,
        })
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

impl Debug for BroadcastListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<BroadcastListener = {}>", self.listen_addr)
    }
}
