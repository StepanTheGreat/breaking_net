use breaking_net::{BroadcastListener, BroadcastWriter};

const PORT: u16 = 5555;

fn make_listener(port: u16) -> BroadcastListener {
    BroadcastListener::new(port).unwrap()
}

fn make_writer(port: u16) -> BroadcastWriter {
    BroadcastWriter::new(port).unwrap()
}

/// Test common broadcasting use-cases
#[test]
fn test_broadcasts() {
    let mut listener1 = make_listener(PORT);
    let mut listener2 = make_listener(PORT);
    let mut writer = make_writer(PORT);

    // No packets as of now
    assert!(!listener1.has_packets());
    assert!(!listener2.has_packets());    

    let data = [1, 2, 3, 4];

    // Send some data to all of them
    writer.send(&data).unwrap();

    // Packets should be available now
    assert!(listener1.has_packets());
    assert!(listener2.has_packets());

    {
        // Receive the packets
        let (msg1, addr1) = listener1.recv().unwrap();
        let (msg2, addr2) = listener2.recv().unwrap();
        
        assert_eq!(msg1, msg2); // Assert that the messages are identical
        assert_eq!(addr1, addr2); // The senders are identical

        assert_eq!(&msg1, &data); // The message perfectly matches what was sent
    }

    // They should no longer have any packets
    assert!(!listener1.has_packets());
    assert!(!listener2.has_packets());
}