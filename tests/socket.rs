use std::net;
use breaking_net::*;

const ADDR_A: net::SocketAddr = socket_addr!(localhost; 0);
const ADDR_B: net::SocketAddr = socket_addr!(localhost; 0);
const ADDR_C: net::SocketAddr = socket_addr!(localhost; 0);

const DT: f32 = 1.0/30.0;

macro_rules! poll_socks {
    ($dt:expr, $($sock:expr),*) => {
        $(
            ($sock).poll($dt);
        )*
    };
}

#[test]
fn test_basic_sockets() {
    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    let msg = b"Hello";
    let rel = Reliability::Unreliable;

    // Send our message
    sock_a.send_to(&ADDR_B, msg, rel);

    // Poll our sockets
    poll_socks!(DT, sock_a, sock_b);

    // Ensure that our data is correct
    let packet = sock_b.recv_from().unwrap();
    assert_eq!(packet.sender, ADDR_A);
    assert_eq!(packet.data, msg);
    assert_eq!(packet.reliability, rel);
}