//! Test basic broadcast usage

use std::net;

use breaking_net::*;

const PORT: u16 = 5555;
const LISTENER_ADDR: net::SocketAddr = socket_addr!(unspecified; PORT);
const WRITER_ADDR: net::SocketAddr = socket_addr!(unspecified; 0);

fn make_listener() -> BroadcastListener {
    BroadcastListener::new(LISTENER_ADDR).unwrap()
}

fn make_writer() -> BroadcastWriter {
    BroadcastWriter::new(WRITER_ADDR, PORT).unwrap()
}

/// Test common broadcasting use-cases
#[test]
fn test_broadcasts() {
    let mut listener1 = make_listener();
    let mut listener2 = make_listener();
    let mut writer = make_writer();

    // No messages as of now
    assert!(!listener1.has_messages());
    assert!(!listener2.has_messages());

    let data = [1, 2, 3, 4];

    // Send some data to all of them
    writer.send(&data).unwrap();

    // Packets should be available now
    assert!(listener1.has_messages());
    assert!(listener2.has_messages());

    {
        // Receive the messages
        let (msg1, addr1) = listener1.recv().unwrap();
        let (msg2, addr2) = listener2.recv().unwrap();

        assert_eq!(msg1, msg2); // Assert that the messages are identical
        assert_eq!(addr1, addr2); // The senders are identical

        assert_eq!(&msg1, &data); // The message perfectly matches what was sent
    }

    // They should no longer have any messages
    assert!(!listener1.has_messages());
    assert!(!listener2.has_messages());
}
