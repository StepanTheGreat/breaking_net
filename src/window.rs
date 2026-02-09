use std::fmt::{Display, write};

use crate::packet::PacketSeqId;


#[cfg(test)] 
type BitPage = u16;

#[cfg(not(test))]
type BitPage = u64;

const PAGE_BITS: usize = BitPage::BITS as _;

#[derive(Clone)]
pub struct BitSet {
    pages: Box<[BitPage]>,
    len: usize,
}

impl BitSet {
    pub fn new(len: usize) -> Self {
        assert!(len > 0, "A bit array can't have zero frames");

        let pages = vec![0; len].into_boxed_slice();
        
        Self {
            len,
            pages
        }
    }

    /// Set a bit at the provided location
    pub fn set(&mut self, index: usize, to: bool) {
        let value = self.get(index);

        let offset = index % PAGE_BITS;

        if value != to {
            self.pages[index] ^= 1 << offset; 
        }
    }

    /// Directly put an entire page at a page position
    pub fn put(&mut self, index: usize, page: BitPage) {
        self.pages[index] = page;
    } 

    pub fn read(&self, index: usize) -> BitPage {
        self.pages[index]
    }

    /// Get the value of the provided bit (indexed by bit index)
    pub fn get(&mut self, index: usize) -> bool {
        assert!(index/PAGE_BITS < self.len);

        let offset = index % PAGE_BITS;

        (
            self.pages[index/PAGE_BITS] & (1 << offset)
        ) > 0
    }

    /// Shift this structure to the right
    pub fn shr(&mut self, by: usize) {

        // The value with all bits as 1. This allows us to "clear out" sections to which we're going to move our new shifted values
        let eraser = BitPage::MAX;

        // For each set (iterating from the left)
        for i in (0..self.len).rev() {

            // Get the current page
            let page = self.pages[i];
            
            // The current position in bits
            let bind = i * PAGE_BITS;

            // If the offset is larger than our bit index (meaning that our page will go out of bounds)
            // In any other case we can safely move it to any position
            let new_bind = bind+by;

            let page_offset = new_bind % PAGE_BITS;

            // Our new position is page aligned, meaning that we can directly overwrite the previous page 
            if page_offset == 0 {
                let new_ind = new_bind / PAGE_BITS;

                // Directly overwrite it
                self.pages[new_ind] = page;
            } else {
                // In any other case we will have to touch multiple pages
                let new_a_ind = new_bind.div_euclid(PAGE_BITS);
                let new_b_ind = new_bind.div_ceil(PAGE_BITS);

                if new_a_ind >= self.len {
                    continue;
                } 

                // Erase our A page from the left
                self.pages[new_a_ind] |= eraser << page_offset;
                self.pages[new_a_ind] ^= eraser << page_offset;

                // Write our new page
                self.pages[new_a_ind] |= page >> page_offset;


                if new_b_ind >= self.len {
                    continue;
                }

                // Repeat for B
                self.pages[new_b_ind] |= eraser >> (PAGE_BITS-page_offset);
                self.pages[new_b_ind] ^= eraser >> (PAGE_BITS-page_offset);

                self.pages[new_b_ind] |= page << (PAGE_BITS-page_offset);
                
            }
        }

        // When we shift to the left
        self.pages[0] |= eraser << (PAGE_BITS.saturating_sub(by));
        self.pages[0] ^= eraser << (PAGE_BITS.saturating_sub(by));
    }

    pub fn as_ref(&self) -> &[BitPage] {
        &self.pages
    }
}

impl Display for BitSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for page in self.pages.iter() {
            write!(f, "{page:016b}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::window::BitSet;

    #[test]
    fn test_single_bitset() {
        let mut page: u128 = 0xF4F1748182F917A1293FA11283;
        let mut bitset = BitSet::new(8);

        // Load page by page our enormous u128 bitset
        for ind in 0..8 {
            bitset.put(ind, (page >> (7-ind)*16) as u16);
        }

        println!("Page:   {page:0128b}");
        println!("Bitset: {bitset}");
        println!();

        for shift_by in [1, 2, 3, 7, 50, 90] {
            page = page.unbounded_shr(shift_by);
            bitset.shr(shift_by as _);

            println!("Shifting by {shift_by}:");
            println!("Page:   {page:0128b}");
            println!("Bitset: {bitset}");

            for ind in 0..8 {
                let page_a = bitset.read(ind);
                let page_b = (page >> (7-ind)*16) as u16;

                println!("{ind} = {page_a:04x} / {page_b:04x} (expected)");
                assert_eq!(page_a, page_b);
            }
        }
    }
}

/// The sliding window structure that helps keeping track of acknowledged packets.
/// 
/// The way it works, is the window contains the general offset (the latest packet), and frames that go **before** this offset packet. It can be
/// visualised like so: (latest)[frame_1][frame_2][frame_3][frame_n]
/// 
/// When we add a new packet, we compare it against our latest packet. 
/// - If it's larger - we have to shift our entire structure to the left, based on the
///   delta between the new packet and the latest one. After that we must mark our former packet the same way.
/// - If it's smaller - we must mark a bit in one of the available windows. If it's further than that - we won't mark anything.
/// - In any other case we don't do anything. 
pub struct SlidingAckWindow {
    /// The latest packet to arrive
    latest: Option<PacketSeqId>,
    
    /// The amount of packet frames (a single frame can contain multiple packets)
    frames_amount: usize,

    /// The frame storage itself (has a constant size)
    frames: Box<[u64]>
}

impl SlidingAckWindow {
    /// Create a new sliding window with the provided amount of frames. 
    /// 
    /// Note that a single frame contains multiple packets (64 to be precise)
    pub fn new(frames_amount: usize) -> Self {
        let frames = vec![0; frames_amount].into_boxed_slice();

        Self { 
            latest: None, 
            frames_amount, 
            frames
        }
    }

    pub fn mark(&mut self, packet: PacketSeqId) {
        
        match self.latest {
            None => {
                self.latest = Some(packet);
            },
            Some(latest) => {
                if packet > latest {
                    let delta = packet-latest;
                    
                }
            }
        }
        if self.latest.is_none() {
            self.latest = Some(packet);
        }
    }
}