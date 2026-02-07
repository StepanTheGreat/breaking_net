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

TODO: Explain more the innerworkings of the protocol (how it received packets, acknowledges them, sends them and so on)

## Implementation

### Socket
A socket is simply an abstraction over a UDP socket that has a few differences:
1. It's by default non-blocking. All operations on them are non-blocking, which makes it MUCH more convenient for independent applications.
   It instead requires manual polling from the user, with supplied delta time. Using this delta it can:
      1. Update its inner timers (for example resend timers)
      2. Understand its limits (for example how many packets it can send per a single poll)
2. It has a maximum MTU (Maximum Transport Unit) which doesn't support any fragmentation (for now). Under the hood, there are 2 levels of MTU:
   - Public (user MTU)
   - Private (protocol MTU)

   The protocol's MTU is just slighly larger to accomodate for headers and other stuff. The reason why there's a difference, is because having a common MTU
   would be confusing. The user using the entire capacity of the same MTU would literally mean that the protocol wouldn't have any space for its own metadata.
   That's why there are slightly different numbers, exactly for that reason.
3. It by default batches multiple smaller packets into larger **crates**. **Crates** are protocol-level packets that simply include lists of metadata
   (like acknowledgments and smaller packets). This achieves a few things:
   1. It reduces the overall packet rate (which in turn doesn't overwhelm the network as much)
   2. It reduces the chances of packet loss (compare the chances of reliably receiving 4 different packets, over receiving a single one)
   
   Because PPS (Packets Per Second) metric is customizable - our sockets only try to fit as much data as possible into the available PPS during the poll (thanks
   to **delta time**). 

   The way it works, is: at each poll, our socket is going to calculate the amount of packets it can send (for example, with 120PPS and 30TPS, per single tick we
   can send up to 4 packets in total). Then, it's going to iterate the queue of packets. For each packet, it checks if it can fit into a crate. If it can - it
   goes directly there, increasing its overall size. In any other case it goes into the `cant_fit` queue, packets in which will get re-added later.

   After we're done with packets - we're going to try fit acknowledgments. Absolutely the same way.

   Note that we always first prioritize packets, and only THEN acknowledgments. It's absolutely possible for us to never send any acknowledgments at all, if the remaining packet size is always less than our size of an acknowledgment (though theoretically we should be able to send at least 2 per packet, thanks to our private MTU size increase). 
