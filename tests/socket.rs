use std::net;
use breaking_net::*;

const ADDR_A: net::SocketAddr = socket_addr!(localhost; 0);
const ADDR_B: net::SocketAddr = socket_addr!(localhost; 0);
// const ADDR_C: net::SocketAddr = socket_addr!(localhost; 0);

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

    // We're going to send 4 messages to each other
    let msgs: [&[u8]; _] = [
        b"Hello",
        b" ",
        b"World",
        b"!",
    ];
    let rel = Reliability::Unreliable;

    // Send them 1 by 1
    for msg in msgs {
        sock_a.send_to(&sock_b.addr(), msg, rel);
        sock_b.send_to(&sock_a.addr(), msg, rel);
    }

    // Poll our sockets
    poll_socks!(DT, sock_a, sock_b);
    {
        // Only one crate should be sent (all packets should be concatenated)
        assert_eq!(sock_a.sent_crates(), 1);
        assert_eq!(sock_b.sent_crates(), 1);
    
        // Only one crate should be received (for the same reason)
        assert_eq!(sock_b.received_crates(), 1);
    }

    // We're going to poll one more time, because socket A couldn't receive B's crate, since it was sent after A was polled
    poll_socks!(DT, sock_a);
    assert_eq!(sock_a.received_crates(), 1);

    // Both of them should have packets available
    assert!(sock_a.has_packets());
    assert!(sock_b.has_packets());

    // Ensure that our data is correct
    for msg in msgs {
        
        // Receive from socket A (with B as the sender)
        {
            let packet = sock_a.recv_from().unwrap();
            assert_eq!(packet.sender, sock_b.addr());
            assert_eq!(packet.data, msg);
            assert_eq!(packet.reliability, rel);
        }

        // Receive from socket B (with A as the sender)
        {
            let packet = sock_b.recv_from().unwrap();
            assert_eq!(packet.sender, sock_a.addr());
            assert_eq!(packet.data, msg);
            assert_eq!(packet.reliability, rel);
        }
    }

    // Make sure that none of them has any other packets to discover
    assert!(!sock_b.has_packets());
    assert!(!sock_a.has_packets());
}

#[test]
fn test_corruption_detection() {
    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    let msg = b"Hello";
    let rel = Reliability::Unreliable;

    // Guarantee corruption
    set_packed_corruption_chance(1.0);

    // Send our message
    sock_a.send_to(&ADDR_B, msg, rel);

    // Poll our sockets
    poll_socks!(DT, sock_a, sock_b);

    // Ensure that our socket DOESN'T receive that packet
    assert!(sock_b.recv_from().is_none());

    set_packed_corruption_chance(0.0);
}