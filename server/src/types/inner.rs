use alloy::primitives::Address;

use super::Level;
use crate::{
    order_book::{
        Oid,
        types::{Coin, InnerOrder, Px, Side, Sz},
    },
    prelude::*,
    types::{L4Order, OrderDiff, node_data::NodeDataOrderStatus},
};

// L4Order: the struct we keep in the orderbook (computationally better)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InnerL4Order {
    pub user: Address,
    pub coin: Coin,
    pub side: Side,
    pub limit_px: Px,
    pub sz: Sz,
    pub oid: u64,
    pub timestamp: u64,
    pub trigger_condition: String,
    pub is_trigger: bool,
    pub trigger_px: String,
    pub is_position_tpsl: bool,
    pub reduce_only: bool,
    pub order_type: String,
    pub tif: Option<String>,
    pub cloid: Option<String>,
}

impl InnerOrder for InnerL4Order {
    fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    fn side(&self) -> Side {
        self.side
    }

    fn limit_px(&self) -> Px {
        self.limit_px
    }

    fn sz(&self) -> Sz {
        self.sz
    }

    fn decrement_sz(&mut self, dec: Sz) {
        self.sz.decrement_sz(dec.value());
    }

    fn modify_px(&mut self, px: Px) {
        self.limit_px = px;
    }

    fn modify_sz(&mut self, sz: Sz) {
        self.sz = sz;
    }

    fn fill(&mut self, maker_order: &mut Self) -> Sz {
        let match_sz = self.sz().min(maker_order.sz());
        self.decrement_sz(match_sz);
        maker_order.decrement_sz(match_sz);
        match_sz
    }

    fn convert_trigger(&mut self, ts: u64) {
        if self.is_trigger {
            self.trigger_px = "0.0".to_string();
            self.trigger_condition = "Triggered".to_string();
            self.is_trigger = false;
            self.timestamp = ts;
            self.tif = Some("Gtc".to_string());
        }
    }

    fn coin(&self) -> Coin {
        self.coin.clone()
    }
}

impl TryFrom<(Address, L4Order)> for InnerL4Order {
    type Error = Error;

    fn try_from(value: (Address, L4Order)) -> Result<Self> {
        let L4Order {
            coin,
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid,
            ..
        } = value.1;
        let user = value.0;
        let limit_px = Px::parse_from_str(&limit_px)?;
        let sz = Sz::parse_from_str(&sz)?;
        Ok(Self {
            user,
            coin: Coin::new(&coin),
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid,
        })
    }
}

impl From<InnerL4Order> for L4Order {
    fn from(value: InnerL4Order) -> Self {
        let InnerL4Order {
            user,
            coin,
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid,
        } = value;
        let limit_px = limit_px.to_str();
        let sz_str = sz.to_str();
        Self {
            user: Some(user),
            coin: coin.value(),
            side,
            limit_px,
            sz: sz_str.clone(),
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            children: Vec::new(),
            is_position_tpsl,
            reduce_only,
            order_type,
            orig_sz: sz_str,
            tif,
            cloid,
        }
    }
}

impl TryFrom<NodeDataOrderStatus> for InnerL4Order {
    type Error = Error;

    fn try_from(value: NodeDataOrderStatus) -> Result<Self> {
        (value.user, value.order).try_into()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InnerLevel {
    pub px: Px,
    pub sz: Sz,
    pub n: usize,
}

impl From<InnerLevel> for Level {
    fn from(value: InnerLevel) -> Self {
        Self::new(value.px.to_str(), value.sz.to_str(), value.n)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum InnerOrderDiff {
    New { sz: Sz },
    Update { _orig_sz: Sz, new_sz: Sz },
    Remove,
}

impl TryFrom<&OrderDiff> for InnerOrderDiff {
    type Error = Error;

    fn try_from(value: &OrderDiff) -> Result<Self> {
        Ok(match value {
            OrderDiff::New { sz } => Self::New { sz: Sz::parse_from_str(sz)? },
            OrderDiff::Update { orig_sz, new_sz } => {
                Self::Update { _orig_sz: Sz::parse_from_str(orig_sz)?, new_sz: Sz::parse_from_str(new_sz)? }
            }
            OrderDiff::Remove => Self::Remove,
        })
    }
}

impl TryFrom<OrderDiff> for InnerOrderDiff {
    type Error = Error;

    fn try_from(value: OrderDiff) -> Result<Self> {
        (&value).try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::types::{Coin, InnerOrder, Px, Side, Sz};
    use alloy::primitives::Address;

    fn make_l4_order(coin: &str, side: Side, px: &str, sz: &str, oid: u64) -> L4Order {
        L4Order {
            user: None,
            coin: coin.to_string(),
            side,
            limit_px: px.to_string(),
            sz: sz.to_string(),
            oid,
            timestamp: 1000,
            trigger_condition: "N/A".to_string(),
            is_trigger: false,
            trigger_px: "0.0".to_string(),
            children: Vec::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Limit".to_string(),
            orig_sz: sz.to_string(),
            tif: Some("Gtc".to_string()),
            cloid: None,
        }
    }

    // ==================== InnerL4Order Conversion Tests ====================

    #[test]
    fn test_l4order_to_inner_l4order() {
        let addr = Address::new([1; 20]);
        let l4 = make_l4_order("BTC", Side::Bid, "50000.5", "1.25", 42);
        let inner: InnerL4Order = (addr, l4).try_into().unwrap();
        assert_eq!(inner.coin(), Coin::new("BTC"));
        assert_eq!(inner.side(), Side::Bid);
        assert_eq!(inner.limit_px(), Px::parse_from_str("50000.5").unwrap());
        assert_eq!(inner.sz(), Sz::parse_from_str("1.25").unwrap());
        assert_eq!(inner.oid(), crate::order_book::Oid::new(42));
        assert_eq!(inner.user, addr);
    }

    #[test]
    fn test_inner_l4order_to_l4order_roundtrip() {
        let addr = Address::new([2; 20]);
        let original = make_l4_order("ETH", Side::Ask, "3000.12345678", "0.5", 99);
        let inner: InnerL4Order = (addr, original).try_into().unwrap();
        let back: L4Order = inner.into();

        assert_eq!(back.coin, "ETH");
        assert_eq!(back.side, Side::Ask);
        assert_eq!(back.limit_px, "3000.12345678");
        assert_eq!(back.sz, "0.5");
        assert_eq!(back.oid, 99);
        assert_eq!(back.user, Some(addr));
    }

    #[test]
    fn test_inner_l4order_invalid_price() {
        let addr = Address::new([0; 20]);
        let mut l4 = make_l4_order("BTC", Side::Bid, "50000", "1.0", 1);
        l4.limit_px = "not_a_number".to_string();
        let result: Result<InnerL4Order> = (addr, l4).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_inner_l4order_invalid_size() {
        let addr = Address::new([0; 20]);
        let mut l4 = make_l4_order("BTC", Side::Bid, "50000", "1.0", 1);
        l4.sz = "invalid".to_string();
        let result: Result<InnerL4Order> = (addr, l4).try_into();
        assert!(result.is_err());
    }

    // ==================== InnerOrder Trait Tests ====================

    #[test]
    fn test_inner_order_fill() {
        let addr = Address::new([0; 20]);
        let l4a = make_l4_order("BTC", Side::Bid, "50000", "3.0", 1);
        let l4b = make_l4_order("BTC", Side::Ask, "49000", "2.0", 2);
        let mut taker: InnerL4Order = (addr, l4a).try_into().unwrap();
        let mut maker: InnerL4Order = (addr, l4b).try_into().unwrap();

        let filled = taker.fill(&mut maker);
        assert_eq!(filled, Sz::parse_from_str("2.0").unwrap());
        assert_eq!(taker.sz(), Sz::parse_from_str("1.0").unwrap());
        assert_eq!(maker.sz().value(), 0);
    }

    #[test]
    fn test_inner_order_modify_sz() {
        let addr = Address::new([0; 20]);
        let l4 = make_l4_order("BTC", Side::Bid, "50000", "1.0", 1);
        let mut inner: InnerL4Order = (addr, l4).try_into().unwrap();
        inner.modify_sz(Sz::parse_from_str("5.0").unwrap());
        assert_eq!(inner.sz(), Sz::parse_from_str("5.0").unwrap());
    }

    #[test]
    fn test_convert_trigger() {
        let addr = Address::new([0; 20]);
        let mut l4 = make_l4_order("BTC", Side::Bid, "50000", "1.0", 1);
        l4.is_trigger = true;
        l4.trigger_condition = "tp".to_string();
        l4.trigger_px = "51000".to_string();
        let mut inner: InnerL4Order = (addr, l4).try_into().unwrap();

        assert!(inner.is_trigger);
        inner.convert_trigger(999);
        assert!(!inner.is_trigger);
        assert_eq!(inner.trigger_px, "0.0");
        assert_eq!(inner.trigger_condition, "Triggered");
        assert_eq!(inner.timestamp, 999);
        assert_eq!(inner.tif, Some("Gtc".to_string()));
    }

    #[test]
    fn test_convert_trigger_noop_when_not_trigger() {
        let addr = Address::new([0; 20]);
        let l4 = make_l4_order("BTC", Side::Bid, "50000", "1.0", 1);
        let mut inner: InnerL4Order = (addr, l4).try_into().unwrap();
        let orig_timestamp = inner.timestamp;
        inner.convert_trigger(999);
        assert_eq!(inner.timestamp, orig_timestamp); // unchanged
    }

    // ==================== InnerOrderDiff Tests ====================

    #[test]
    fn test_order_diff_new() {
        let diff = OrderDiff::New { sz: "1.5".to_string() };
        let inner: InnerOrderDiff = diff.try_into().unwrap();
        assert!(matches!(inner, InnerOrderDiff::New { sz } if sz == Sz::parse_from_str("1.5").unwrap()));
    }

    #[test]
    fn test_order_diff_update() {
        let diff = OrderDiff::Update { orig_sz: "2.0".to_string(), new_sz: "1.0".to_string() };
        let inner: InnerOrderDiff = diff.try_into().unwrap();
        assert!(matches!(inner, InnerOrderDiff::Update { new_sz, .. } if new_sz == Sz::parse_from_str("1.0").unwrap()));
    }

    #[test]
    fn test_order_diff_remove() {
        let diff = OrderDiff::Remove;
        let inner: InnerOrderDiff = diff.try_into().unwrap();
        assert!(matches!(inner, InnerOrderDiff::Remove));
    }

    #[test]
    fn test_order_diff_invalid_sz() {
        let diff = OrderDiff::New { sz: "invalid".to_string() };
        let result: Result<InnerOrderDiff> = diff.try_into();
        assert!(result.is_err());
    }

    // ==================== InnerLevel Tests ====================

    #[test]
    fn test_inner_level_to_level() {
        let inner = InnerLevel {
            px: Px::parse_from_str("100.5").unwrap(),
            sz: Sz::parse_from_str("2.5").unwrap(),
            n: 3,
        };
        let level: Level = inner.into();
        assert_eq!(level.px(), "100.5");
        assert_eq!(level.sz(), "2.5");
    }
}
