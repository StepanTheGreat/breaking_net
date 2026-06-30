//! Test basic socket interactions

use bnet::*;
use std::{net, thread, time::Duration};

const ADDR_A: net::SocketAddr = socket_addr!(localhost; 0);
const ADDR_B: net::SocketAddr = socket_addr!(localhost; 0);
// const ADDR_C: net::SocketAddr = socket_addr!(localhost; 0);

// 30 times per second
const DT: Duration = Duration::from_millis(33);

/// A mini constructor that automatically creates virtual sockets
fn make_socket(addr: net::SocketAddr) -> Socket {
    Socket::new_ex(
        addr,
        SocketOptions {
            virtual_socket: true,
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn test_basic_sockets() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

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

    // Both of them should have messages available
    assert!(sock_a.has_messages());
    assert!(sock_b.has_messages());

    // Ensure that our data is correct
    for msg in msgs {
        // Receive from socket A (with B as the sender)
        {
            let message = sock_a.recv_from().unwrap();
            assert_eq!(message.sender, sock_b.addr());
            assert_eq!(message.data, msg);
            assert_eq!(message.reliability, rel);
        }

        // Receive from socket B (with A as the sender)
        {
            let message = sock_b.recv_from().unwrap();
            assert_eq!(message.sender, sock_a.addr());
            assert_eq!(message.data, msg);
            assert_eq!(message.reliability, rel);
        }
    }

    // Make sure that none of them has any other messages to discover
    assert!(!sock_b.has_messages());
    assert!(!sock_a.has_messages());
}

#[test]
fn test_mtu_limits() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

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

    // Make sure that none of them has any other messages to discover
    assert!(!sock_b.has_messages());
    assert!(!sock_a.has_messages());
}

#[test]
fn test_corruption_detection() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

    let msg = b"Hi";
    let rel = Reliability::Unreliable;

    // Send our message
    sock_a.send_to(&sock_b.addr(), msg, rel);

    // Poll our sockets
    poll_socks!(DT, [sock_a, sock_b]);

    // Ensure that our socket receives that message
    assert!(sock_b.recv_from().is_some());

    // Guarantee corruption
    sock_a.virtual_settings().unwrap().set_corruption_rate(1.0);

    // Send our message
    sock_a.send_to(&sock_b.addr(), msg, rel);

    // Poll our sockets
    poll_socks!(DT, [sock_a, sock_b]);

    // Ensure that our socket DOESN'T receive that message
    assert!(sock_b.recv_from().is_none());
}

#[test]
fn test_reliable_messages() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

    let msg = b"Hello";
    let rel = Reliability::Reliable;

    // Guarantee message loss
    sock_a.virtual_settings().unwrap().set_packet_loss_rate(1.0);

    // Connect them
    sock_a.connect(sock_b.addr());
    sock_b.connect(sock_a.addr());

    // Send our message
    sock_a.send_to(&sock_b.addr(), msg, rel);

    // It will fail no matter how many times we're going to resend it
    poll_socks!(5, DT, [sock_a, sock_b]);
    assert!(!sock_b.has_messages());

    // Drop our message loss
    sock_a.virtual_settings().unwrap().set_packet_loss_rate(0.0);

    // Poll even more (due to exponential backoff)
    poll_socks!(20, DT, [sock_a, sock_b]);

    // Ensure that our socket receives the message
    assert!(sock_b.recv_from().is_some());

    // Receive it only once
    assert!(!sock_b.has_messages());
}

#[test]
fn test_deduplication_messages() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

    let msg = b"Hello";

    // Guarantee message loss
    sock_a.virtual_settings().unwrap().set_dublicate_rate(1.0);

    // First we're going to connect them together, since messages without a connection never get "filtered"
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    // Send our message
    sock_a.send_to(&sock_b.addr(), msg, Reliability::ReliableUnordered);

    // Poll 10 times
    poll_socks!(10, DT, [sock_a, sock_b]);

    // Ensure that our socket receives the message
    assert!(sock_b.recv_from().is_some());

    // Receive it only once
    assert!(!sock_b.has_messages());
}

#[test]
fn test_reordering_messages() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

    let msgs: &[&[u8]] = &[b"Hello", b" ", b"World", b"!"];

    // Let's throw some horrible numbers there
    sock_a.virtual_settings().unwrap().set_dublicate_rate(1.0);

    // First we're going to connect them together, since messages without a connection never get "filtered"
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
        let message = sock_b.recv_from().unwrap();

        assert!(&message.data == msg);
    }

    // Receive it only once
    assert!(!sock_b.has_messages());
}

/// Test a continous message dialogue.
/// This test in particular tests if sockets can handle large volumes of messages in less packets.
///
/// This test can sometimes fail due to randomness, which is somewhat expected due to randomness involved
#[test]
fn test_continuous_reliable_dialogue() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

    // First we're going to connect them together
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    const LATENCY: u64 = 120;
    const JITTER: u64 = 20;

    // Let's throw some horrible numbers there
    sock_b
        .virtual_settings()
        .unwrap()
        .set_dublicate_rate(1.0)
        .set_latency(Duration::from_millis(LATENCY))
        .set_jitter(Duration::from_millis(JITTER));

    sock_a
        .virtual_settings()
        .unwrap()
        .set_dublicate_rate(1.0)
        .set_latency(Duration::from_millis(LATENCY))
        .set_jitter(Duration::from_millis(JITTER));

    const MESSAGE_LEN: usize = 220;
    const MESSAGES: u8 = 200;

    // How much time is allocated to this test
    const MAX_TIME: Duration = Duration::from_millis((MESSAGES / 5) as u64 * (LATENCY + JITTER));

    // Queue A LOT of messages
    for msg_ind in 0..MESSAGES {
        let msg = [msg_ind; MESSAGE_LEN];

        sock_a.send_to(&sock_b.addr(), &msg, Reliability::Reliable);
        sock_b.send_to(&sock_a.addr(), &msg, Reliability::Reliable);
    }

    let mut counter_a = 0;
    let mut counter_b = 0;
    let mut time = Duration::ZERO;

    while counter_a < MESSAGES || counter_b < MESSAGES {
        time += DT;
        if time > MAX_TIME {
            panic!("Out of time");
        }

        sock_a.poll(DT);

        while let Some(packet) = sock_a.recv_from() {
            assert_eq!(packet.data[0], counter_a);
            counter_a += 1;
        }

        sock_b.poll(DT);

        while let Some(packet) = sock_b.recv_from() {
            assert_eq!(packet.data[0], counter_b);
            counter_b += 1;
        }

        assert!(sock_a.is_connected(&sock_b.addr()));
        assert!(sock_b.is_connected(&sock_a.addr()));

        thread::sleep(Duration::from_millis(8));
    }

    assert_eq!(counter_a, MESSAGES);
    assert_eq!(counter_b, MESSAGES);

    assert!(!sock_a.has_messages());
    assert!(!sock_b.has_messages());
}

/// Test if heartbeat
#[test]
fn test_heartbeat() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

    // First we're going to connect them together
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    // Clear out connection events
    sock_a.get_event().unwrap();
    sock_b.get_event().unwrap();

    assert!(sock_a.is_connected(&sock_b.addr()));
    assert!(sock_b.is_connected(&sock_a.addr()));

    // Poll them for 10 while seconds (more than enough to time out)
    poll_socks!(Duration::from_secs(10), [sock_a, sock_b]);

    // They should be no longer corrected
    assert!(!sock_a.is_connected(&sock_b.addr()));
    assert!(!sock_b.is_connected(&sock_a.addr()));

    assert_eq!(
        sock_a.get_event().unwrap(),
        SocketEvent::Disconnection(sock_b.addr())
    );
    assert_eq!(
        sock_b.get_event().unwrap(),
        SocketEvent::Disconnection(sock_a.addr())
    );
}

/// Test round trip calculations
#[test]
fn test_round_trip_time() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

    // First we're going to connect them together
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    // The round trip time should be 0 right at the start
    // assert_eq!(sock_a.round_trip_time(sock_b.addr()).unwrap(), 0.0);

    let msg = b"Test message";

    // Send a message to B
    sock_a.send_to(&sock_b.addr(), msg, Reliability::Reliable);

    // Poll our socket 3 times, without polling B (to simulate delay from B)
    poll_socks!(2, DT, [sock_a]);

    // Finally, actually poll both of them
    poll_socks!(DT, [sock_b, sock_a]);

    // Receive the message
    sock_b.recv_from().unwrap();

    // The round trip time must be larger than DT
    let rtt = sock_a.statistics_for(&sock_b.addr()).unwrap().rtt;
    assert!(rtt > DT.as_secs_f64());
}

/// One problem on purely unreliable connections... they can't measure RTT, because there are no reliable packets.
/// And it's super problematic, since it doesn't allow us to figure out network conditions, pacing and so on.
///
/// The protocol must ensure that at least once in a while, reliable packets are sent, even when there are no reliable messages
#[test]
fn test_infrequent_rtt_measurements() {
    let mut sock_a = make_socket(ADDR_A);
    let mut sock_b = make_socket(ADDR_B);

    let msg = b"Test message";

    // First we're going to connect them together
    sock_b.connect(sock_a.addr());
    sock_a.connect(sock_b.addr());

    let a_rtt = sock_a.statistics_for(&sock_b.addr()).unwrap().rtt;
    let b_rtt = sock_b.statistics_for(&sock_a.addr()).unwrap().rtt;

    const POLLS: usize = 200;

    for _ in 0..POLLS {
        sock_a.send_to(&sock_b.addr(), msg, Reliability::Unreliable);
        sock_b.send_to(&sock_a.addr(), msg, Reliability::Unreliable);

        poll_socks!(DT, [sock_a, sock_b]);

        thread::sleep(Duration::from_millis(8));
    }

    // They must gain new statistics
    assert_ne!(a_rtt, sock_a.statistics_for(&sock_b.addr()).unwrap().rtt);
    assert_ne!(b_rtt, sock_b.statistics_for(&sock_a.addr()).unwrap().rtt);
}
