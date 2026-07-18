use super::*;

#[test]
fn strategy_config_default_is_futures() {
    let cfg = StrategyConfig::default();
    assert_eq!(cfg.trade_type, TradeType::Futures);
    assert!(cfg.shorting_allowed());
}

#[test]
fn spot_trade_type_disables_shorting_and_leverage() {
    let cfg = StrategyConfig::new("15m").with_trade_type(TradeType::Spot);
    assert!(!cfg.shorting_allowed());
    assert_eq!(cfg.leverage_default, 1.0);
    assert_eq!(cfg.leverage_max, 1.0);
}

#[test]
fn parse_timeframe_minutes() {
    assert_eq!(StrategyConfig::parse_timeframe("15m"), 15);
    assert_eq!(StrategyConfig::parse_timeframe("1h"), 60);
    assert_eq!(StrategyConfig::parse_timeframe("1d"), 1440);
}
