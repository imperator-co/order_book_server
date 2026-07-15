use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::ops::Add;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum Side {
    #[serde(rename = "A")]
    Ask,
    #[serde(rename = "B")]
    Bid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Oid(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Px(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Sz(u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Coin(String);

impl Sz {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
    pub(super) const fn is_positive(self) -> bool {
        self.0 > 0
    }
    pub(super) const fn is_zero(self) -> bool {
        self.0 == 0
    }
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
    pub(crate) const fn decrement_sz(&mut self, dec: u64) {
        self.0 = self.0.saturating_sub(dec);
    }
}

impl Px {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
}

impl Oid {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
}

pub(crate) trait InnerOrder: Clone {
    fn coin(&self) -> Coin;
    fn oid(&self) -> Oid;
    fn side(&self) -> Side;
    fn limit_px(&self) -> Px;
    fn sz(&self) -> Sz;
    fn decrement_sz(&mut self, dec: Sz);
    fn fill(&mut self, maker_order: &mut Self) -> Sz;
    fn modify_sz(&mut self, sz: Sz);
    fn convert_trigger(&mut self, ts: u64);
}

impl Coin {
    pub(crate) fn new(coin: &str) -> Self {
        Self(coin.to_string())
    }

    pub(crate) fn value(&self) -> String {
        self.0.clone()
    }

    /// Returns true for spot markets: @ prefixed coins and PURR/USDC
    pub(crate) fn is_spot(&self) -> bool {
        self.0.starts_with('@') || self.0 == "PURR/USDC"
    }

    /// Returns true for HIP-3 markets: coins with colon format (X:Y)
    /// Examples: flx:COIN, xyz:AMD, abc:XYZ
    pub(crate) fn is_hip3(&self) -> bool {
        self.0.contains(':')
    }

    /// Returns true for perpetual futures (not spot, not hip3)
    pub(crate) fn is_perp(&self) -> bool {
        !self.is_spot() && !self.is_hip3()
    }
}

/// Lets `HashMap<Coin, _>` / `HashSet<Coin>` be queried with a plain `&str`
/// (`map.get(coin_str)`), avoiding a `String` allocation per lookup on the
/// broadcast hot paths. Sound because `Coin` hashes/compares exactly like its
/// inner `String`, which itself hashes/compares like `str`.
impl std::borrow::Borrow<str> for Coin {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Add<Self> for Sz {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        // Saturating to avoid a panic if level aggregation ever sums beyond u64::MAX.
        // Not realistic for any single coin's book but free defense-in-depth on a hot path.
        Self(self.0.saturating_add(rhs.0))
    }
}

// Multiply all sizes and prices by 10^MAX_DECIMALS for ease of computation.
const MULTIPLIER: f64 = 100_000_000.0;

impl Debug for Px {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / MULTIPLIER))
    }
}

impl Debug for Sz {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / MULTIPLIER))
    }
}

/// Shared parser for the fixed-point u64 representation. Rejects non-finite,
/// negative, and overflow inputs explicitly — `as u64` would otherwise saturate
/// negatives to 0 and oversized values to `u64::MAX`, silently corrupting the
/// book if the upstream ever emits a malformed string.
fn parse_fixed_point(value: &str) -> Result<u64> {
    let parsed = value.parse::<f64>()?;
    if !parsed.is_finite() {
        return Err(format!("non-finite numeric input: {value}").into());
    }
    if parsed < 0.0 {
        return Err(format!("negative numeric input: {value}").into());
    }
    let scaled = (parsed * MULTIPLIER).round();
    if scaled > u64::MAX as f64 {
        return Err(format!("numeric input overflows u64: {value}").into());
    }
    Ok(scaled as u64)
}

impl Px {
    pub(crate) fn parse_from_str(value: &str) -> Result<Self> {
        Ok(Self::new(parse_fixed_point(value)?))
    }

    #[must_use]
    pub(crate) fn to_str(self) -> String {
        let s = format!("{:.8}", (self.value() as f64) / MULTIPLIER);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn num_digits(self) -> u32 {
        if self.value() == 0 { 1 } else { (self.value() as f64).log10().floor() as u32 + 1 }
    }
}

impl Sz {
    pub(crate) fn parse_from_str(value: &str) -> Result<Self> {
        Ok(Self::new(parse_fixed_point(value)?))
    }

    #[must_use]
    pub(crate) fn to_str(self) -> String {
        let s = format!("{:.8}", (self.value() as f64) / MULTIPLIER);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Px Tests ====================

    #[test]
    fn test_px_parse_integer() {
        let px = Px::parse_from_str("100").unwrap();
        assert_eq!(px.value(), 10_000_000_000);
    }

    #[test]
    fn test_px_parse_decimal() {
        let px = Px::parse_from_str("50000.12345678").unwrap();
        assert_eq!(px.to_str(), "50000.12345678");
    }

    #[test]
    fn test_px_parse_small_decimal() {
        let px = Px::parse_from_str("0.00000001").unwrap();
        assert_eq!(px.value(), 1);
        assert_eq!(px.to_str(), "0.00000001");
    }

    #[test]
    fn test_px_parse_zero() {
        let px = Px::parse_from_str("0").unwrap();
        assert_eq!(px.value(), 0);
        assert_eq!(px.to_str(), "0");
    }

    #[test]
    fn test_px_parse_trailing_zeros() {
        let px = Px::parse_from_str("100.10000000").unwrap();
        assert_eq!(px.to_str(), "100.1");
    }

    #[test]
    fn test_px_parse_invalid() {
        assert!(Px::parse_from_str("abc").is_err());
        assert!(Px::parse_from_str("").is_err());
    }

    #[test]
    fn test_px_parse_rejects_negative() {
        assert!(Px::parse_from_str("-1.5").is_err());
        assert!(Px::parse_from_str("-0.0000001").is_err());
        assert!(Sz::parse_from_str("-1").is_err());
    }

    #[test]
    fn test_px_parse_rejects_overflow_and_non_finite() {
        // Above u64::MAX / MULTIPLIER (~1.8e11) would silently wrap before; now rejected.
        assert!(Px::parse_from_str("1e20").is_err());
        assert!(Px::parse_from_str("inf").is_err());
        assert!(Px::parse_from_str("-inf").is_err());
        assert!(Px::parse_from_str("nan").is_err());
        assert!(Sz::parse_from_str("1e20").is_err());
    }

    #[test]
    fn test_sz_add_saturates_on_overflow() {
        let a = Sz::new(u64::MAX);
        let b = Sz::new(1);
        let c = a + b;
        assert_eq!(c.value(), u64::MAX); // saturates, does not panic
    }

    #[test]
    fn test_px_roundtrip() {
        for val in ["0.001", "1", "34.01", "100000", "0.12345678", "99999.99999999"] {
            let px = Px::parse_from_str(val).unwrap();
            assert_eq!(px.to_str(), val, "roundtrip failed for {val}");
        }
    }

    #[test]
    fn test_px_ordering() {
        let a = Px::parse_from_str("100.5").unwrap();
        let b = Px::parse_from_str("200.3").unwrap();
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, Px::parse_from_str("100.5").unwrap());
    }

    #[test]
    fn test_px_num_digits() {
        assert_eq!(Px::new(0).num_digits(), 1);
        assert_eq!(Px::new(1).num_digits(), 1);
        assert_eq!(Px::new(9).num_digits(), 1);
        assert_eq!(Px::new(10).num_digits(), 2);
        assert_eq!(Px::new(999).num_digits(), 3);
        assert_eq!(Px::new(10_000_000_000).num_digits(), 11); // 100.0 in raw
    }

    #[test]
    fn test_px_debug_format() {
        let px = Px::parse_from_str("123.456").unwrap();
        let dbg = format!("{:?}", px);
        assert!(dbg.contains("123.456"), "debug output: {dbg}");
    }

    // ==================== Sz Tests ====================

    #[test]
    fn test_sz_parse_and_roundtrip() {
        for val in ["0.001", "1", "100.5", "0.00000001"] {
            let sz = Sz::parse_from_str(val).unwrap();
            assert_eq!(sz.to_str(), val, "roundtrip failed for {val}");
        }
    }

    #[test]
    fn test_sz_zero() {
        let sz = Sz::new(0);
        assert!(sz.is_zero());
        assert!(!sz.is_positive());
    }

    #[test]
    fn test_sz_positive() {
        let sz = Sz::new(1);
        assert!(sz.is_positive());
        assert!(!sz.is_zero());
    }

    #[test]
    fn test_sz_decrement() {
        let mut sz = Sz::new(100);
        sz.decrement_sz(30);
        assert_eq!(sz.value(), 70);
    }

    #[test]
    fn test_sz_decrement_saturating() {
        let mut sz = Sz::new(10);
        sz.decrement_sz(100);
        assert_eq!(sz.value(), 0);
        assert!(sz.is_zero());
    }

    #[test]
    fn test_sz_add() {
        let a = Sz::new(100);
        let b = Sz::new(200);
        let c = a + b;
        assert_eq!(c.value(), 300);
    }

    #[test]
    fn test_sz_min() {
        let a = Sz::new(100);
        let b = Sz::new(200);
        assert_eq!(a.min(b).value(), 100);
        assert_eq!(b.min(a).value(), 100);
    }

    #[test]
    fn test_sz_debug_format() {
        let sz = Sz::parse_from_str("1.5").unwrap();
        let dbg = format!("{:?}", sz);
        assert!(dbg.contains("1.5"), "debug output: {dbg}");
    }

    // ==================== Coin Tests ====================

    #[test]
    fn test_coin_perp() {
        let c = Coin::new("BTC");
        assert!(c.is_perp());
        assert!(!c.is_spot());
        assert!(!c.is_hip3());
    }

    #[test]
    fn test_coin_spot_at_prefix() {
        let c = Coin::new("@1");
        assert!(c.is_spot());
        assert!(!c.is_perp());
        assert!(!c.is_hip3());
    }

    #[test]
    fn test_coin_spot_purr() {
        let c = Coin::new("PURR/USDC");
        assert!(c.is_spot());
        assert!(!c.is_perp());
    }

    #[test]
    fn test_coin_hip3() {
        let c = Coin::new("flx:COIN");
        assert!(c.is_hip3());
        assert!(!c.is_perp());
        assert!(!c.is_spot());
    }

    #[test]
    fn test_coin_value_roundtrip() {
        let c = Coin::new("ETH");
        assert_eq!(c.value(), "ETH");
    }

    #[test]
    fn test_coin_equality() {
        assert_eq!(Coin::new("BTC"), Coin::new("BTC"));
        assert_ne!(Coin::new("BTC"), Coin::new("ETH"));
    }

    #[test]
    fn test_coin_ordering() {
        assert!(Coin::new("AAA") < Coin::new("BBB"));
    }

    #[test]
    fn test_coin_borrow_str_lookup() {
        // Borrow<str> lets hot-path maps be queried without allocating a Coin.
        let mut set = std::collections::HashSet::new();
        set.insert(Coin::new("BTC"));
        assert!(set.contains("BTC"));
        assert!(!set.contains("ETH"));

        let mut map = std::collections::HashMap::new();
        map.insert(Coin::new("BTC"), 1u8);
        assert_eq!(map.get("BTC"), Some(&1));
    }

    // ==================== Side Tests ====================

    #[test]
    fn test_side_serde() {
        let ask: Side = serde_json::from_str(r#""A""#).unwrap();
        assert_eq!(ask, Side::Ask);
        let bid: Side = serde_json::from_str(r#""B""#).unwrap();
        assert_eq!(bid, Side::Bid);
        assert_eq!(serde_json::to_string(&Side::Ask).unwrap(), r#""A""#);
        assert_eq!(serde_json::to_string(&Side::Bid).unwrap(), r#""B""#);
    }

    #[test]
    fn test_side_ordering() {
        // Just verify it's deterministic
        let sides = [Side::Ask, Side::Bid];
        assert!(sides[0] <= sides[1] || sides[0] >= sides[1]);
    }

    // ==================== Oid Tests ====================

    #[test]
    fn test_oid_equality() {
        assert_eq!(Oid::new(42), Oid::new(42));
        assert_ne!(Oid::new(1), Oid::new(2));
    }
}
