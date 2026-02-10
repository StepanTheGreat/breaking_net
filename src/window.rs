use std::fmt::Display;

use crate::packet::PacketSeqId;


#[cfg(test)] 
/// For testing we're using 16bit uints because we're testing against std's u128 integer (which in turn can fit up to 8 pages at once, making it
/// perfect for testing)
type BitPage = u16;

#[cfg(not(test))]
/// In development though, it makes more sense to use larger pages, like 64bit uints
type BitPage = u64;

/// The amount of bits in our bit page
const PAGE_BITS: usize = BitPage::BITS as _;

/// A super minimal bitset implementation which allows setting bits at arbitrary positions and shifting the entire structure to the right 
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
    pub fn shr(&mut self, mut by: usize) {

        // When we're shifting pages, we can shift by entire integers at once. This is a pretty slow operation, 
        if by >= PAGE_BITS {

            // By how many pages to shift
            let shift_pages = by / PAGE_BITS;

            // Decrement by an entire page
            by -= shift_pages * PAGE_BITS;
            
            // For each page index, starting from 1
            for ind in (0..self.len).rev() {
                let new_ind = ind + shift_pages;

                if new_ind >= self.len {
                    continue;
                }

                // Swap it with the page to its left
                self.pages[new_ind] = self.pages[ind];
            }

            // The first pages will simply turn into zeros
            for ind in 0..shift_pages {
                self.pages[ind] = 0;
            }
        }

        // For each set (iterating from the right)
        for i in (0..self.len).rev() {

            // If it's the last page - simply shift it 
            if i == self.len-1 {
                self.pages[i] >>= by;
            } else {
                // In any other case we're going to shift our current page onto the right one
                self.pages[i+1] |= self.pages[i] << (PAGE_BITS-by);

                // And now shift our own page
                self.pages[i] >>= by;
            }
        }
    }

    pub fn as_ref(&self) -> &[BitPage] {
        &self.pages
    }
}

#[cfg(test)]
mod tests {
    use crate::window::{BitPage, BitSet, PAGE_BITS};

    #[test]
    fn test_bitset() {
        const TEST_PAGES: usize = (u128::BITS as usize) / PAGE_BITS;

        // Load our structure
        let o_page: u128 = 0xF4F1748182F917A1293FA11283;
        let o_bitset = {   
            let mut bs = BitSet::new(TEST_PAGES);

            // Load page by page our enormous u128 bitset
            for ind in 0..TEST_PAGES {
                bs.put(ind, (o_page >> ((TEST_PAGES-1)-ind)*PAGE_BITS) as BitPage);
            }

            bs
        };

        // For every shift amount
        for shift_by in [1, 2, 3, 7, 50, 90, 124] {
            
            // Shift our 2 structures
            let page = o_page.unbounded_shr(shift_by);
            let mut bitset = o_bitset.clone();
            bitset.shr(shift_by as _);

            // Then compare them page by page
            for ind in 0..TEST_PAGES {
                let page_a = bitset.read(ind);
                let page_b = (page >> ((TEST_PAGES-1)-ind)*PAGE_BITS) as BitPage;

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