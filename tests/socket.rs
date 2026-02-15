use breaking_net::*;
use std::net;

const ADDR_A: net::SocketAddr = socket_addr!(localhost; 0);
const ADDR_B: net::SocketAddr = socket_addr!(localhost; 0);
// const ADDR_C: net::SocketAddr = socket_addr!(localhost; 0);

const DT: f32 = 1.0 / 30.0;

#[test]
fn test_basic_sockets() {
    // Before each test we should ensure to first reset the stress environment settings
    reset_stress_environment();

    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    // We're going to send 4 messages to each other
    let msgs: [&[u8]; _] = [b"Hello", b" ", b"World", b"!"];
    let rel = Reliability::Unreliable;

    // Send them 1 by 1
    for msg in msgs {
        sock_a.send_to(&sock_b.addr(), msg, rel);
        sock_b.send_to(&sock_a.addr(), msg, rel);
    }

    // Poll our sockets 2 times (due to ordering reasons)
    poll_socks!(2, DT, [sock_a, sock_b]);

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
    reset_stress_environment();

    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    let msg = b"Hello";
    let rel = Reliability::Unreliable;

    // Send our message
    sock_a.send_to(&sock_b.addr(), msg, rel);

    // Poll our sockets
    poll_socks!(DT, [sock_a, sock_b]);

    // Ensure that our socket DOESN'T receive that packet
    assert!(sock_b.recv_from().is_some());

    // Guarantee corruption
    set_packed_corruption_chance(1.0);

    // Send our message
    sock_a.send_to(&sock_b.addr(), msg, rel);

    // Poll our sockets
    poll_socks!(DT, [sock_a, sock_b]);

    // Ensure that our socket DOESN'T receive that packet
    assert!(sock_b.recv_from().is_none());
}

#[test]
fn test_reliable_packets() {
    reset_stress_environment();

    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    let msg = b"Hello";
    let rel = Reliability::Reliable;

    // Guarantee packet loss
    set_packet_loss_chance(1.0);

    // Connect them
    sock_a.connect(sock_b.addr());
    sock_b.connect(sock_a.addr());

    // Send our message
    sock_a.send_to(&sock_b.addr(), msg, rel);

    // It will fail no matter how many times we're going to resend it
    poll_socks!(10, DT, [sock_a, sock_b]);
    assert!(!sock_b.has_packets());

    // Drop our packet loss
    set_packet_loss_chance(0.0);

    // Poll 10 times
    poll_socks!(10, DT, [sock_a, sock_b]);

    // Ensure that our socket receives the packet
    assert!(sock_b.recv_from().is_some());

    // Receive it only once
    assert!(!sock_b.has_packets());
}

#[test]
fn test_deduplication_packets() {
    reset_stress_environment();

    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    let msg = b"Hello";

    // Guarantee packet loss
    set_packed_dublication_chance(1.0);

    // First we're going to connect them together, since packets without a connection never get "filtered"
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    // Send our message
    sock_a.send_to(&sock_b.addr(), msg, Reliability::ReliableUnordered);

    // Poll 10 times
    poll_socks!(10, DT, [sock_a, sock_b]);

    // Ensure that our socket receives the packet
    assert!(sock_b.recv_from().is_some());

    // Receive it only once
    assert!(!sock_b.has_packets());
}

#[test]
fn test_reordering_packets() {
    reset_stress_environment();

    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    let msgs: &[&[u8]] = &[b"Hello", b" ", b"World", b"!"];

    // Let's throw some horrible numbers there
    set_packet_reorder_chance(1.0);
    set_packed_dublication_chance(1.0);

    // First we're going to connect them together, since packets without a connection never get "filtered"
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    // Send our message reliably
    for msg in msgs {
        sock_a.send_to(&sock_b.addr(), msg, Reliability::Reliable);
    }

    // Poll 10 times
    poll_socks!(10, DT, [sock_a, sock_b]);

    // Finally, for each message that we sent
    for msg in msgs {
        // Receive it and check for the contents
        let packet = sock_b.recv_from().unwrap();

        assert!(&packet.data == msg);
    }

    // Receive it only once
    assert!(!sock_b.has_packets());
}

/// Test a continous message dialogue
#[test]
fn test_hundreds_of_packets() {
    reset_stress_environment();

    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    // First we're going to connect them together
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    // Let's throw some horrible numbers there
    set_packet_reorder_chance(1.0);
    set_packed_dublication_chance(1.0);

    const N: u32 = 258;

    // Let's send 258 integers
    for num in 0..=N {
        // We're going to send them to each other
        sock_a.send_to(&sock_b.addr(), &num.to_be_bytes(), Reliability::Reliable);
        sock_b.send_to(&sock_a.addr(), &num.to_be_bytes(), Reliability::Reliable);

        poll_socks!(DT, [sock_a, sock_b]);
    }

    // One final poll
    poll_socks!(DT, [sock_a, sock_b]);

    // Now receive and check
    for num in 0..=N {
        let pack_b = sock_a.recv_from().unwrap().data;
        let pack_a = sock_b.recv_from().unwrap().data;

        assert_eq!(&pack_b, &num.to_be_bytes());
        assert_eq!(&pack_b, &pack_a);
    }

    assert!(!sock_a.has_packets());
    assert!(!sock_b.has_packets());
}
