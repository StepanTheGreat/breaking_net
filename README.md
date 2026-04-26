# breaking_net

A RUDP crate with a minimal set of dependencies. This one in particular implements reliable sockets over UDP (with different levels of reliability) + some broadcasting primitives. This is especially useful in dynamic applications that sometimes don't care about reliability (something that you can't prevent with TCP). This crate was inspired by [laminar](https://github.com/TimonPost/laminar), which unfortunately seems to be no longer actively developped.

You can read the roadmap below to get a sense of what this crate is supposed to be, but essentially:
1. This crate should be simple to use, like `laminar`.
2. The protocol should take care of some of the complexity that is usually expected to be implemented by the user. In particular: packet batching and encryption (the latter being one of the most important features of this protocol).
3. While initially planned for WLAN communications (which are super forgiving) - making a somewhat efficient protocol for highly-dynamic WAN applications would be also great.
4. Should easily support sending/receiving broadcast messages.
5. Should be usable on native platforms (desktop/mobile).
6. Should be customizable enough for most use-cases (for example: custom packet rates, MTU limits and so on).

## Example
```rust
use std::time::Duration;
use bnet::{Socket, Reliability};

// Our ports here are constant. You can easily get an OS assigned one by using `0` instead 
let addr_a = "127.0.0.1:7878".parse().unwrap();
let addr_b = "127.0.0.1:8787".parse().unwrap();

// Create AND bind our sockets to these addresses
let mut sock_a = Socket::new(addr_a).unwrap();
let mut sock_b = Socket::new(addr_b).unwrap();

// Let's send an unreliable message from socket A to B
let msg = b"hello";
sock_a.send_to(&addr_b, msg, Reliability::Unreliable);

// Poll both of them by 33 milliseconds (so A could send a message, and B could received it)
sock_a.poll(Duration::from_millis(33));
sock_b.poll(Duration::from_millis(33));

// Receive the message and test its contents
let received = sock_b.recv_from().unwrap();

assert_eq!(received.data, msg);
assert_eq!(received.sender, addr_a);

// Let's send a message back to establish a two-way connection
sock_b.send_to(&addr_a, msg, Reliability::Unreliable);

// Poll again, in reversed order
sock_b.poll(Duration::from_millis(33));
sock_a.poll(Duration::from_millis(33));

// Both now should be connected to each other
assert!(sock_a.is_connected(&addr_b));
assert!(sock_b.is_connected(&addr_a));
```

## The roadmap
- [x] Unreliable channel
- [x] Reliable unordered channel
- [x] Reliable ordered channel
- [x] Packet batching
- [x] Better acknowledgments (using bitmaps)
- [x] Protocol versioning
- [x] Heartbeat management
- [x] Socket events (instead of simply packets)
- [x] Better time management
- [ ] Fragmentation
- [ ] Congestion control + RTT
- [ ] Basic DoS prevention
- [ ] Encryption + Handshake (probably using `snow`)
- [ ] Socket customization (custom PPS, MTU limits, ...)
- [ ] Better connection management (via UIDs, better reconnection)
- [ ] QUIC-like acknowledgment 
- [ ] Better stress testing
- [ ] Benchmarking 
- [ ] C ABI 

## Testing
A super obvious note if you would like to locally build and test the crate yourself: disable your firewall. Might be obvious, but I personally sunked a lot of
time trying to fix something that wasn't broken. 

## Contributing
This is a beginner attempt at network programming, due to lack of an **actively maintained**, rust game-networking protocol; as expected - it's super pritimive and not the most clever protocol you can find. However, any contribution to the project and attempt to improve it will be highly appreciated (simply testing it on different platforms, analyzing its memory consumption, stress testing and so on). 

## Licensing
MIT / Apache 2.0, under your choice.
