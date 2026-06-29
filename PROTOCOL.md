# Protocol specification
This file is a collection of my notes regarding the implementation of the protocol. Here I will detail its features, how it's supposed to work and so on.

First of all, the protocol was inspired by [laminar](https://github.com/TimonPost/laminar), thus some design choices were borrowed directly from said project.

The idea behind this protocol is to let the user decide how to structure their connection infrastructure.
While the socket must be able to efficiently combine and send data - things like connection management, heartbeat and so on are user's responsibilities, thus
reducing the protocol's overall complexity, while also giving more freedom to the user.

One of the main priorities of this protocol is to be able to:
1. Support multiple reliability settings:
    - Unreliable (a packet can be lost or dublicated or out of order)
    - Unreliable ordered (a packet is ordered, but still can be lost or dublicated)
    - Reliable unordered (a packet is guaranteed to get received, but in unspecified order)
    - Reliable ordered (a packet is guaranteed to get received in a specific order)
2. Support fragmentation (even if limited to particular reliability settings). This will allow the user to JUST send large packets and don't worry about
   their reconstruction. This is a protocol feature, without which the user would have to essentially build a protocol on top of a protocol. Note that if not
   needed - the user should be able to easily send non-fragmented packets, which are marginally simpler to both send and receive.
3. Support packet batching. This is a protocol feature, because under the hood, the protocol already included small data like akcnowledgments with packets.
   This would allow the user to not worry about packet count (because codebase simplicity is valuable). Things like heartbeats become essentially free thanks to
   this feature.

   Note that batching is performed ONLY when the packet's size is less than the MTU size (thus some space can be filled with important data).
4. Implement some sort of congestion control. While not super important - being able to analyze the network and adapt to it is super beneficial. One obvious
   example would be adapting to its MTU size, which in turn can lead to better batching results. Packet rate can be adapted depending on the status of the 
   network. Overall, a good feature, but not the priority for now.
5. Support basic levels of encryption. This is a responsibility of the protocol, because:
   1. User-implemented encryption wouldn't encrypt crate metadata
   2. Encrypting individual packets would explode the total batched size of said packets.
   3. Decrypting each packet, instead of a single crate would be marginally slower

   For these reasons, encryption must be implemented on the protocol's level. This in turn might change the way some interactions are implemented (for example
   some connection must be first established to be able to send packets to an unknown socket)


TODO: Document the protocol with all its features and innerworkings. 