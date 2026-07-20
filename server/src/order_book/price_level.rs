use crate::order_book::{
    Oid, Sz,
    linked_list::LinkedList,
    types::{InnerOrder, Px},
};

/// A single price level: the FIFO order queue plus a running size aggregate.
///
/// `total_sz` is maintained incrementally on every mutation so BBO and L2
/// builds read the level's (sum, count) in O(1) instead of folding over every
/// order - the fold dominated L2 rebuild cost on deep books. All size-mutating
/// access goes through this type; the inner `LinkedList` is never handed out
/// mutably, so the aggregate cannot be bypassed.
#[derive(Clone)]
pub(crate) struct PriceLevel<O> {
    orders: LinkedList<Oid, O>,
    total_sz: u64,
}

impl<O: InnerOrder> PriceLevel<O> {
    pub(crate) fn new() -> Self {
        Self { orders: LinkedList::new(), total_sz: 0 }
    }

    /// Append an order. The aggregate is bumped only when the push succeeded -
    /// a duplicate oid is rejected by the list and must not inflate the sum.
    pub(crate) fn push_back(&mut self, oid: Oid, order: O) -> bool {
        let sz = order.sz().value();
        let inserted = self.orders.push_back(oid, order);
        if inserted {
            self.total_sz = self.total_sz.saturating_add(sz);
        }
        self.debug_validate();
        inserted
    }

    /// Insert an order directly in front of `before` in the queue. The aggregate
    /// is bumped only when the splice succeeded - a missing anchor or duplicate
    /// oid is rejected by the list and must not inflate the sum.
    pub(crate) fn insert_before(&mut self, before: &Oid, oid: Oid, order: O) -> bool {
        let sz = order.sz().value();
        let inserted = self.orders.insert_before(before, oid, order);
        if inserted {
            self.total_sz = self.total_sz.saturating_add(sz);
        }
        self.debug_validate();
        inserted
    }

    /// Whether an order with this oid is resting at this level.
    pub(crate) fn contains(&self, oid: &Oid) -> bool {
        self.orders.contains_key(oid)
    }

    /// Remove an order by id, decrementing the aggregate by its current size.
    pub(crate) fn remove(&mut self, oid: Oid) -> Option<O> {
        let removed = self.orders.remove_node(oid);
        if let Some(order) = &removed {
            self.total_sz = self.total_sz.saturating_sub(order.sz().value());
        }
        self.debug_validate();
        removed
    }

    /// Change an order's size in place, applying the delta to the aggregate.
    /// Updates can increase as well as decrease the size.
    pub(crate) fn modify_order_sz(&mut self, oid: &Oid, sz: Sz) -> bool {
        let Some(order) = self.orders.node_value_mut(oid) else {
            return false;
        };
        let old = order.sz().value();
        order.modify_sz(sz);
        let new = order.sz().value();
        self.total_sz = self.total_sz.saturating_sub(old).saturating_add(new);
        self.debug_validate();
        true
    }

    /// Match the taker against this level's queue, FIFO (the caller has already
    /// validated that the level's price crosses the taker). Fully-filled maker
    /// oids are pushed into `filled_oids` and removed from the level. Stops
    /// when the taker is exhausted or the level is empty.
    ///
    /// The aggregate delta is the maker's size before minus after `fill` -
    /// deliberately NOT `fill`'s return value, whose contract does not
    /// guarantee that it equals the maker's decrement.
    pub(crate) fn match_against(&mut self, taker_order: &mut O, filled_oids: &mut Vec<Oid>) {
        loop {
            let Some(maker) = self.orders.head_value_ref_mut_unsafe() else {
                break;
            };
            let before = maker.sz().value();
            taker_order.fill(maker);
            let after = maker.sz().value();
            let maker_filled = maker.sz().is_zero();
            let maker_oid = maker.oid();
            self.total_sz = self.total_sz.saturating_sub(before.saturating_sub(after));
            if maker_filled {
                filled_oids.push(maker_oid);
                let _unused = self.orders.remove_front();
            }
            if taker_order.sz().is_zero() {
                break;
            }
        }
        self.debug_validate();
    }

    /// O(1) running sum of all order sizes at this level.
    pub(crate) const fn total_sz(&self) -> Sz {
        Sz::new(self.total_sz)
    }

    /// O(1) order count at this level.
    pub(crate) fn len(&self) -> usize {
        self.orders.len()
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub(crate) fn to_vec(&self) -> Vec<&O> {
        self.orders.to_vec()
    }

    /// Compaction preserves the order set, so the aggregate is unaffected.
    pub(crate) fn compact(&mut self) -> bool {
        let compacted = self.orders.compact();
        self.debug_validate();
        compacted
    }

    pub(crate) fn slab_len(&self) -> usize {
        self.orders.slab_len()
    }

    pub(crate) fn slab_capacity(&self) -> usize {
        self.orders.slab_capacity()
    }

    /// Debug-only invariant check: the running aggregate must equal the fold
    /// over live orders. Called at the end of every mutator, so every test that
    /// exercises the book doubles as an aggregate-invariant test.
    #[inline]
    fn debug_validate(&self) {
        #[cfg(debug_assertions)]
        {
            let folded = self.orders.fold(0_u64, |acc, order| *acc = acc.saturating_add(order.sz().value()));
            debug_assert_eq!(folded, self.total_sz, "PriceLevel aggregate drifted from the fold sum");
        }
    }
}

/// (price, total size, order count) read in O(1) - the BBO/L2 building block.
pub(crate) fn aggregate_entry<O: InnerOrder>((px, level): (&Px, &PriceLevel<O>)) -> (Px, Sz, u32) {
    (*px, level.total_sz(), u32::try_from(level.len()).unwrap_or(u32::MAX))
}
