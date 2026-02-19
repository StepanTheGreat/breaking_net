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

    sock_a.connect(sock_b.addr());
    sock_b.connect(sock_a.addr());

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
fn test_mtu_limits() {
    reset_stress_environment();

    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    // We're going to send 1 large message itilizing ALL our MTU capacity
    let message = &[0u8; MTU_SIZE];
    let rel = Reliability::Reliable;

    sock_a.connect(sock_b.addr());
    sock_b.connect(sock_a.addr());

    // Send them 1 by 1
    sock_a.send_to(&sock_b.addr(), message, rel);
    sock_b.send_to(&sock_a.addr(), message, rel);

    // Poll our sockets 2 times (due to ordering reasons)
    poll_socks!(2, DT, [sock_a, sock_b]);

    assert_eq!(sock_a.recv_from().unwrap().data, message);
    assert_eq!(sock_b.recv_from().unwrap().data, message);

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

    // Ensure that our socket receives that packet
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
fn test_continuous_reliable_dialogue() {
    reset_stress_environment();

    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    // First we're going to connect them together
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    // Let's throw some horrible numbers there
    set_packet_reorder_chance(1.0);
    set_packed_dublication_chance(1.0);

    const MESSAGE_LEN: usize = 220;
    const MESSAGES: u8 = 20;
    const MESSAGES_PER_ITER: u8 = 4;

    // For this amount of messages
    let mut messages = MESSAGES;

    while messages > 0 {

        // Send multiple messages per single iteration
        for _ in 0..MESSAGES_PER_ITER {
            let msg = [messages; MESSAGE_LEN]; 

            sock_a.send_to(&sock_b.addr(), &msg, Reliability::Reliable);
            sock_b.send_to(&sock_a.addr(), &msg, Reliability::Reliable);

            messages -= 1;
        }

        // Poll them
        poll_socks!(DT, [sock_a, sock_b, sock_a]);
    }

    // Now receive and check
    let mut messages = MESSAGES;
    while sock_a.has_packets() || sock_b.has_packets() {
        poll_socks!(DT, [sock_a, sock_b, sock_a]);

        let pack_b = sock_a.recv_from().unwrap().data;
        let pack_a = sock_b.recv_from().unwrap().data;

        assert_eq!(pack_b[0], messages);

        assert_eq!(&pack_b, &pack_a);

        messages -= 1;
    }

    assert!(!sock_a.has_packets());
    assert!(!sock_b.has_packets());
}


/// Test a continous message dialogue
#[test]
fn test_round_trip_time() {
    reset_stress_environment();

    let mut sock_a = Socket::new(ADDR_A).unwrap();
    let mut sock_b = Socket::new(ADDR_B).unwrap();

    // First we're going to connect them together
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    // The round trip time should be 0 right at the start
    assert_eq!(sock_a.round_trip_time(sock_b.addr()).unwrap(), 0.0);

    let msg = b"Test message";
    
    // Send a packet to B
    sock_a.send_to(&sock_b.addr(), msg, Reliability::Reliable);

    // Poll our socket 3 times, without polling B (to simulate delay from B)
    poll_socks!(2, DT, [sock_a]);

    // Finally, actually poll both of them
    poll_socks!(DT, [sock_b, sock_a]);

    // Receive the packet
    sock_b.recv_from().unwrap();

    // The round trip time must be larger than 0 
    assert!(sock_a.round_trip_time(sock_b.addr()).unwrap() > 0.0);


}
