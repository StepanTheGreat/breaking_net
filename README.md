# breaking_net

A RUDP crate with a minimal set of dependencies. This one in particular implements reliable sockets over UDP (with different levels of reliability) + some broadcasting primitives.
This is especially useful in dynamic applications that sometimes don't care about reliability
(something that you can't prevent with TCP). This crate is inspired by [laminar](https://github.com/TimonPost/laminar), which unfortunately is no longer actively developped.

## TODO
- [x] Unreliable channel
- [x] Reliable unordered channel
- [x] Reliable ordered channel
- [x] Packet batching
- [ ] Heartbeat management
- [ ] Better acknowledgments (using bitmaps)
- [ ] Fragmentation
- [ ] Protocol versioning
- [ ] Congestion control
- [ ] Encryption + Hanshake (probably using `snow`) 
- [ ] Socket customization (custom PPS, MTU limits, ...)

## Testing
A super obvious note if you would like to locally build and test the crate yourself: disable your firewall. Might be obvious, but I personally sunked a lot of
time trying to fix something that wasn't broken. 

## Licensing
As always: MIT / Apache 2.0, under your choice.
