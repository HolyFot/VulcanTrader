// =============================================================================
// FAST INDICATORS - Zero-Copy, Incremental, Lock-Free Indicator Engine
// =============================================================================
//
// This module provides ultra-fast indicator computation for live trading:
//
// - Ring buffers for O(1) OHLCV updates
// - Incremental indicators (O(1) per tick instead of O(n))
// - Arc-based sharing for lock-free read access
// - SIMD-friendly memory layout (SoA - Structure of Arrays)
// - No allocations in hot path
//
// Key insight: Only the LAST bar changes on each tick. We track indicator
// state (EMA multipliers, running sums) to compute just the new value.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::collections::HashMap;
use rayon;

/// Maximum bars to keep in ring buffer (powers of 2 for fast modulo)
pub const RING_BUFFER_SIZE: usize = 1024;
pub const RING_BUFFER_MASK: usize = RING_BUFFER_SIZE - 1;

// =============================================================================
// RING BUFFER - Lock-Free OHLCV Storage
// =============================================================================

/// Lock-free ring buffer for price data.
/// Uses atomic sequence counter for consistency checking.
#[repr(align(64))] // Cache line aligned
pub struct OhlcvRingBuffer {
    // Sequence counter for lock-free reads (even = stable, odd = writing)
    sequence: AtomicU64,
    // Current write position
    write_pos: AtomicUsize,
    // Number of valid bars
    bar_count: AtomicUsize,
    
    // Price data - cache line aligned for SIMD
    pub timestamps: Box<[i64; RING_BUFFER_SIZE]>,
    pub open: Box<[f32; RING_BUFFER_SIZE]>,
    pub high: Box<[f32; RING_BUFFER_SIZE]>,
    pub low: Box<[f32; RING_BUFFER_SIZE]>,
    pub close: Box<[f32; RING_BUFFER_SIZE]>,
    pub volume: Box<[f32; RING_BUFFER_SIZE]>,
}

impl OhlcvRingBuffer {
    pub fn new() -> Self {
        Self {
            sequence: AtomicU64::new(0),
            write_pos: AtomicUsize::new(0),
            bar_count: AtomicUsize::new(0),
            timestamps: Box::new([0i64; RING_BUFFER_SIZE]),
            open: Box::new([0.0f32; RING_BUFFER_SIZE]),
            high: Box::new([0.0f32; RING_BUFFER_SIZE]),
            low: Box::new([0.0f32; RING_BUFFER_SIZE]),
            close: Box::new([0.0f32; RING_BUFFER_SIZE]),
            volume: Box::new([0.0f32; RING_BUFFER_SIZE]),
        }
    }
    
    /// Get current sequence number for consistency check
    #[inline(always)]
    pub fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::Acquire)
    }
    
    /// Get number of valid bars
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.bar_count.load(Ordering::Acquire).min(RING_BUFFER_SIZE)
    }
    
    /// Check if empty
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Push a new bar (should only be called from single writer thread)
    #[inline]
    pub fn push(&mut self, ts: i64, o: f32, h: f32, l: f32, c: f32, v: f32) {
        // Increment sequence to odd (writing)
        self.sequence.fetch_add(1, Ordering::Release);
        
        let pos = self.write_pos.load(Ordering::Relaxed);
        let idx = pos & RING_BUFFER_MASK;
        
        // Write data
        self.timestamps[idx] = ts;
        self.open[idx] = o;
        self.high[idx] = h;
        self.low[idx] = l;
        self.close[idx] = c;
        self.volume[idx] = v;
        
        // Update position and count
        self.write_pos.store(pos + 1, Ordering::Release);
        let old_count = self.bar_count.fetch_add(1, Ordering::Release);
        if old_count >= RING_BUFFER_SIZE {
            self.bar_count.store(RING_BUFFER_SIZE, Ordering::Release);
        }
        
        // Increment sequence to even (stable)
        self.sequence.fetch_add(1, Ordering::Release);
    }
    
    /// Update the last bar (for live tick updates within same candle)
    #[inline]
    pub fn update_last(&mut self, h: f32, l: f32, c: f32, v: f32) {
        if self.bar_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        
        self.sequence.fetch_add(1, Ordering::Release);
        
        let pos = self.write_pos.load(Ordering::Relaxed);
        let idx = (pos.saturating_sub(1)) & RING_BUFFER_MASK;
        
        // Update high/low/close/volume
        if h > self.high[idx] { self.high[idx] = h; }
        if l < self.low[idx] { self.low[idx] = l; }
        self.close[idx] = c;
        self.volume[idx] = v;
        
        self.sequence.fetch_add(1, Ordering::Release);
    }
    
    /// Get value at logical index (0 = oldest, len-1 = newest)
    #[inline(always)]
    pub fn get(&self, logical_idx: usize) -> Option<(i64, f32, f32, f32, f32, f32)> {
        let len = self.len();
        if logical_idx >= len {
            return None;
        }
        
        let write_pos = self.write_pos.load(Ordering::Acquire);
        // Calculate physical index
        let start = if write_pos >= len { write_pos - len } else { 0 };
        let phys_idx = (start + logical_idx) & RING_BUFFER_MASK;
        
        Some((
            self.timestamps[phys_idx],
            self.open[phys_idx],
            self.high[phys_idx],
            self.low[phys_idx],
            self.close[phys_idx],
            self.volume[phys_idx],
        ))
    }
    
    /// Get close price at logical index
    #[inline(always)]
    pub fn close_at(&self, logical_idx: usize) -> f32 {
        let len = self.len();
        if logical_idx >= len { return 0.0; }
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let start = if write_pos >= len { write_pos - len } else { 0 };
        let phys_idx = (start + logical_idx) & RING_BUFFER_MASK;
        self.close[phys_idx]
    }
    
    /// Get last N close prices as slice (for batch operations)
    /// Returns (slice, offset) where offset is the starting physical index
    #[inline]
    pub fn last_closes(&self, n: usize) -> Vec<f32> {
        let len = self.len();
        let actual_n = n.min(len);
        let mut result = Vec::with_capacity(actual_n);
        
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let start = if write_pos >= len { write_pos - len } else { 0 };
        
        for i in (len - actual_n)..len {
            let phys_idx = (start + i) & RING_BUFFER_MASK;
            result.push(self.close[phys_idx]);
        }
        result
    }
    
    /// Bulk load from OHLCV data (for initial fill)
    pub fn load_from_ohlcv(&mut self, ohlcv: &[Vec<f64>]) {
        // Reset
        self.write_pos.store(0, Ordering::Release);
        self.bar_count.store(0, Ordering::Release);
        self.sequence.fetch_add(1, Ordering::Release);
        
        for bar in ohlcv.iter().take(RING_BUFFER_SIZE) {
            if bar.len() >= 6 {
                let pos = self.write_pos.load(Ordering::Relaxed);
                let idx = pos & RING_BUFFER_MASK;
                
                self.timestamps[idx] = bar[0] as i64;
                self.open[idx] = bar[1] as f32;
                self.high[idx] = bar[2] as f32;
                self.low[idx] = bar[3] as f32;
                self.close[idx] = bar[4] as f32;
                self.volume[idx] = bar[5] as f32;
                
                self.write_pos.store(pos + 1, Ordering::Release);
                self.bar_count.fetch_add(1, Ordering::AcqRel);
            }
        }
        
        self.sequence.fetch_add(1, Ordering::Release);
    }
}

impl Default for OhlcvRingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// INCREMENTAL INDICATOR STATE
// =============================================================================

/// State for incremental EMA computation
#[derive(Clone, Copy, Default)]
pub struct EmaState {
    pub value: f32,
    pub multiplier: f32,
    pub initialized: bool,
}

impl EmaState {
    #[inline(always)]
    pub fn new(period: usize) -> Self {
        Self {
            value: 0.0,
            multiplier: 2.0 / (period as f32 + 1.0),
            initialized: false,
        }
    }
    
    /// Update with new price, returns new EMA value
    #[inline(always)]
    pub fn update(&mut self, price: f32) -> f32 {
        if !self.initialized {
            self.value = price;
            self.initialized = true;
        } else {
            self.value = price * self.multiplier + self.value * (1.0 - self.multiplier);
        }
        self.value
    }
}

/// State for incremental RSI computation
#[derive(Clone, Copy, Default)]
pub struct RsiState {
    pub avg_gain: f32,
    pub avg_loss: f32,
    pub prev_close: f32,
    pub period: usize,
    pub count: usize,
    pub initialized: bool,
}

impl RsiState {
    pub fn new(period: usize) -> Self {
        Self {
            avg_gain: 0.0,
            avg_loss: 0.0,
            prev_close: 0.0,
            period,
            count: 0,
            initialized: false,
        }
    }
    
    /// Update with new close price, returns RSI value
    #[inline]
    pub fn update(&mut self, close: f32) -> f32 {
        if self.count == 0 {
            self.prev_close = close;
            self.count = 1;
            return 50.0; // Neutral RSI
        }
        
        let change = close - self.prev_close;
        let gain = if change > 0.0 { change } else { 0.0 };
        let loss = if change < 0.0 { -change } else { 0.0 };
        
        if !self.initialized {
            // Accumulating phase
            self.avg_gain += gain;
            self.avg_loss += loss;
            self.count += 1;
            
            if self.count >= self.period {
                self.avg_gain /= self.period as f32;
                self.avg_loss /= self.period as f32;
                self.initialized = true;
            }
        } else {
            // Smoothed update
            let p = self.period as f32;
            self.avg_gain = (self.avg_gain * (p - 1.0) + gain) / p;
            self.avg_loss = (self.avg_loss * (p - 1.0) + loss) / p;
        }
        
        self.prev_close = close;
        
        if !self.initialized {
            return 50.0;
        }
        
        if self.avg_loss < 1e-10 {
            return 100.0;
        }
        
        let rs = self.avg_gain / self.avg_loss;
        100.0 - (100.0 / (1.0 + rs))
    }
}

/// State for incremental MACD computation (3 EMAs)
#[derive(Clone, Default)]
pub struct MacdState {
    pub fast_ema: EmaState,
    pub slow_ema: EmaState,
    pub signal_ema: EmaState,
}

impl MacdState {
    pub fn new(fast: usize, slow: usize, signal: usize) -> Self {
        Self {
            fast_ema: EmaState::new(fast),
            slow_ema: EmaState::new(slow),
            signal_ema: EmaState::new(signal),
        }
    }
    
    /// Update with new price, returns (macd_line, signal_line, histogram)
    #[inline]
    pub fn update(&mut self, price: f32) -> (f32, f32, f32) {
        let fast = self.fast_ema.update(price);
        let slow = self.slow_ema.update(price);
        let macd_line = fast - slow;
        let signal = self.signal_ema.update(macd_line);
        let histogram = macd_line - signal;
        (macd_line, signal, histogram)
    }
}

/// State for incremental ATR computation
#[derive(Clone, Copy, Default)]
pub struct AtrState {
    pub atr: f32,
    pub prev_close: f32,
    pub period: usize,
    pub count: usize,
    pub initialized: bool,
}

impl AtrState {
    pub fn new(period: usize) -> Self {
        Self {
            atr: 0.0,
            prev_close: 0.0,
            period,
            count: 0,
            initialized: false,
        }
    }
    
    /// Update with new OHLC, returns ATR value
    #[inline]
    pub fn update(&mut self, high: f32, low: f32, close: f32) -> f32 {
        if self.count == 0 {
            self.prev_close = close;
            self.atr = high - low; // First TR
            self.count = 1;
            return self.atr;
        }
        
        // True Range
        let tr = (high - low)
            .max((high - self.prev_close).abs())
            .max((low - self.prev_close).abs());
        
        if !self.initialized {
            self.atr += tr;
            self.count += 1;
            
            if self.count >= self.period {
                self.atr /= self.period as f32;
                self.initialized = true;
            }
        } else {
            // Wilder smoothing
            self.atr = (self.atr * (self.period as f32 - 1.0) + tr) / self.period as f32;
        }
        
        self.prev_close = close;
        self.atr
    }
}

/// State for incremental SMA using sliding window sum
#[derive(Clone)]
pub struct SmaState {
    pub sum: f32,
    pub period: usize,
    pub buffer: Vec<f32>,
    pub pos: usize,
    pub count: usize,
}

impl SmaState {
    pub fn new(period: usize) -> Self {
        Self {
            sum: 0.0,
            period,
            buffer: vec![0.0; period],
            pos: 0,
            count: 0,
        }
    }
    
    /// Update with new value, returns SMA
    #[inline]
    pub fn update(&mut self, value: f32) -> f32 {
        if self.count >= self.period {
            self.sum -= self.buffer[self.pos];
        }
        
        self.buffer[self.pos] = value;
        self.sum += value;
        self.pos = (self.pos + 1) % self.period;
        
        if self.count < self.period {
            self.count += 1;
        }
        
        self.sum / self.count as f32
    }
}

/// State for incremental Bollinger Bands
#[derive(Clone)]
pub struct BollingerState {
    pub sma: SmaState,
    pub period: usize,
    pub std_mult: f32,
    pub buffer: Vec<f32>,
    pub pos: usize,
    pub count: usize,
}

impl BollingerState {
    pub fn new(period: usize, std_mult: f32) -> Self {
        Self {
            sma: SmaState::new(period),
            period,
            std_mult,
            buffer: vec![0.0; period],
            pos: 0,
            count: 0,
        }
    }
    
    /// Update with new price, returns (upper, middle, lower, position)
    #[inline]
    pub fn update(&mut self, price: f32) -> (f32, f32, f32, f32) {
        let middle = self.sma.update(price);
        
        // Update buffer for std calculation
        self.buffer[self.pos] = price;
        self.pos = (self.pos + 1) % self.period;
        if self.count < self.period {
            self.count += 1;
        }
        
        // Calculate standard deviation
        let mut sum_sq = 0.0f32;
        let n = self.count.min(self.period);
        for i in 0..n {
            let diff = self.buffer[i] - middle;
            sum_sq += diff * diff;
        }
        let std = (sum_sq / n as f32).sqrt();
        
        let upper = middle + self.std_mult * std;
        let lower = middle - self.std_mult * std;
        let position = if (upper - lower).abs() > 1e-10 {
            (price - lower) / (upper - lower)
        } else {
            0.5
        };
        
        (upper, middle, lower, position)
    }
}

/// State for incremental CCI computation
#[derive(Clone)]
pub struct CciState {
    pub sma: SmaState,
    pub period: usize,
    pub tp_buffer: Vec<f32>,
    pub pos: usize,
    pub count: usize,
}

impl CciState {
    pub fn new(period: usize) -> Self {
        Self {
            sma: SmaState::new(period),
            period,
            tp_buffer: vec![0.0; period],
            pos: 0,
            count: 0,
        }
    }
    
    /// Update with new HLC, returns CCI value
    #[inline]
    pub fn update(&mut self, high: f32, low: f32, close: f32) -> f32 {
        let tp = (high + low + close) / 3.0;
        let sma_tp = self.sma.update(tp);
        
        // Update buffer for MAD calculation
        self.tp_buffer[self.pos] = tp;
        self.pos = (self.pos + 1) % self.period;
        if self.count < self.period {
            self.count += 1;
        }
        
        // Calculate Mean Absolute Deviation
        let n = self.count.min(self.period);
        let mut mad = 0.0f32;
        for i in 0..n {
            mad += (self.tp_buffer[i] - sma_tp).abs();
        }
        mad /= n as f32;
        
        if mad.abs() < 1e-10 {
            return 0.0;
        }
        
        (tp - sma_tp) / (0.015 * mad)
    }
}

/// State for incremental ADX computation
#[derive(Clone)]
pub struct AdxState {
    pub plus_dm_ema: EmaState,
    pub minus_dm_ema: EmaState,
    pub tr_ema: EmaState,
    pub dx_ema: EmaState,
    pub prev_high: f32,
    pub prev_low: f32,
    pub prev_close: f32,
    pub count: usize,
}

impl AdxState {
    pub fn new(period: usize) -> Self {
        Self {
            plus_dm_ema: EmaState::new(period),
            minus_dm_ema: EmaState::new(period),
            tr_ema: EmaState::new(period),
            dx_ema: EmaState::new(period),
            prev_high: 0.0,
            prev_low: 0.0,
            prev_close: 0.0,
            count: 0,
        }
    }
    
    /// Update with new HLC, returns ADX value
    #[inline]
    pub fn update(&mut self, high: f32, low: f32, close: f32) -> f32 {
        if self.count == 0 {
            self.prev_high = high;
            self.prev_low = low;
            self.prev_close = close;
            self.count = 1;
            return 0.0;
        }
        
        // True Range
        let tr = (high - low)
            .max((high - self.prev_close).abs())
            .max((low - self.prev_close).abs());
        
        // Directional Movement
        let up_move = high - self.prev_high;
        let down_move = self.prev_low - low;
        
        let plus_dm = if up_move > down_move && up_move > 0.0 { up_move } else { 0.0 };
        let minus_dm = if down_move > up_move && down_move > 0.0 { down_move } else { 0.0 };
        
        // Smooth with EMA
        let smooth_tr = self.tr_ema.update(tr);
        let smooth_plus_dm = self.plus_dm_ema.update(plus_dm);
        let smooth_minus_dm = self.minus_dm_ema.update(minus_dm);
        
        // Directional Indicators
        let plus_di = if smooth_tr > 0.0 { 100.0 * smooth_plus_dm / smooth_tr } else { 0.0 };
        let minus_di = if smooth_tr > 0.0 { 100.0 * smooth_minus_dm / smooth_tr } else { 0.0 };
        
        // DX
        let di_sum = plus_di + minus_di;
        let dx = if di_sum > 0.0 { 100.0 * (plus_di - minus_di).abs() / di_sum } else { 0.0 };
        
        // ADX (smoothed DX)
        let adx = self.dx_ema.update(dx);
        
        self.prev_high = high;
        self.prev_low = low;
        self.prev_close = close;
        self.count += 1;
        
        adx
    }
}

/// State for incremental MFI computation
#[derive(Clone)]
pub struct MfiState {
    pub period: usize,
    pub pos_flow_buffer: Vec<f32>,
    pub neg_flow_buffer: Vec<f32>,
    pub pos: usize,
    pub count: usize,
    pub prev_tp: f32,
}

impl MfiState {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            pos_flow_buffer: vec![0.0; period],
            neg_flow_buffer: vec![0.0; period],
            pos: 0,
            count: 0,
            prev_tp: 0.0,
        }
    }
    
    /// Update with new HLCV, returns MFI value
    #[inline]
    pub fn update(&mut self, high: f32, low: f32, close: f32, volume: f32) -> f32 {
        let tp = (high + low + close) / 3.0;
        let raw_money_flow = tp * volume;
        
        let (pos_flow, neg_flow) = if self.count == 0 {
            (0.0, 0.0)
        } else if tp > self.prev_tp {
            (raw_money_flow, 0.0)
        } else if tp < self.prev_tp {
            (0.0, raw_money_flow)
        } else {
            (0.0, 0.0)
        };
        
        // Update buffers
        self.pos_flow_buffer[self.pos] = pos_flow;
        self.neg_flow_buffer[self.pos] = neg_flow;
        self.pos = (self.pos + 1) % self.period;
        if self.count < self.period {
            self.count += 1;
        }
        
        self.prev_tp = tp;
        
        // Calculate sums
        let n = self.count.min(self.period);
        let pos_sum: f32 = self.pos_flow_buffer[..n].iter().sum();
        let neg_sum: f32 = self.neg_flow_buffer[..n].iter().sum();
        
        if neg_sum < 1e-10 {
            return 100.0;
        }
        
        let money_ratio = pos_sum / neg_sum;
        100.0 - (100.0 / (1.0 + money_ratio))
    }
}

/// State for incremental ROC computation
#[derive(Clone)]
pub struct RocState {
    pub period: usize,
    pub buffer: Vec<f32>,
    pub pos: usize,
    pub count: usize,
}

impl RocState {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            buffer: vec![0.0; period + 1],
            pos: 0,
            count: 0,
        }
    }
    
    /// Update with new close, returns ROC value
    #[inline]
    pub fn update(&mut self, close: f32) -> f32 {
        // Store current close
        self.buffer[self.pos] = close;
        
        let roc = if self.count >= self.period {
            let old_pos = (self.pos + self.buffer.len() - self.period) % self.buffer.len();
            let old_close = self.buffer[old_pos];
            if old_close > 0.0 {
                ((close - old_close) / old_close) * 100.0
            } else {
                0.0
            }
        } else {
            0.0
        };
        
        self.pos = (self.pos + 1) % self.buffer.len();
        if self.count < self.buffer.len() {
            self.count += 1;
        }
        
        roc
    }
}

/// State for incremental FVG (Fair Value Gap) detection
#[derive(Clone)]
pub struct FvgState {
    pub high_buffer: [f32; 3],
    pub low_buffer: [f32; 3],
    pub pos: usize,
    pub count: usize,
}

impl FvgState {
    pub fn new() -> Self {
        Self { high_buffer: [0.0; 3], low_buffer: [0.0; 3], pos: 0, count: 0 }
    }
    
    /// Update with new HLC, returns FVG indicator (1.0 bullish, -1.0 bearish, 0.0 none)
    #[inline]
    pub fn update(&mut self, high: f32, low: f32) -> f32 {
        self.high_buffer[self.pos] = high;
        self.low_buffer[self.pos] = low;
        let prev_pos = (self.pos + 2) % 3;
        
        let result = if self.count >= 3 {
            let h0 = self.high_buffer[prev_pos];
            let l2 = self.low_buffer[self.pos];
            let l0 = self.low_buffer[prev_pos];
            let h2 = self.high_buffer[self.pos];
            if l2 > h0 { 1.0 } else if h2 < l0 { -1.0 } else { 0.0 }
        } else { 0.0 };
        
        self.pos = (self.pos + 1) % 3;
        if self.count < 3 { self.count += 1; }
        result
    }
}

impl Default for FvgState { fn default() -> Self { Self::new() } }

/// State for incremental VWAP computation
#[derive(Clone)]
pub struct VwapState {
    pub cum_pv: f64,
    pub cum_vol: f64,
}

impl VwapState {
    pub fn new() -> Self { Self { cum_pv: 0.0, cum_vol: 0.0 } }
    
    /// Update with new HLC and volume, returns VWAP
    #[inline]
    pub fn update(&mut self, high: f32, low: f32, close: f32, volume: f32) -> f32 {
        let tp = (high + low + close) as f64 / 3.0;
        self.cum_vol += volume as f64;
        self.cum_pv += tp * volume as f64;
        if self.cum_vol > 0.0 { (self.cum_pv / self.cum_vol) as f32 } else { close }
    }
    
    /// Reset for new session/day
    pub fn reset(&mut self) { self.cum_pv = 0.0; self.cum_vol = 0.0; }
}

impl Default for VwapState { fn default() -> Self { Self::new() } }

/// State for incremental Choppiness Index + Trend Efficiency with Z-scores
#[derive(Clone)]
pub struct CtiState {
    pub atr_1: AtrState,       // 1-period ATR for sum
    pub tr_buffer: Vec<f32>,   // rolling TR values
    pub high_buffer: Vec<f32>, // rolling highs
    pub low_buffer: Vec<f32>,  // rolling lows
    pub close_buffer: Vec<f32>,// rolling closes
    pub chop_buffer: Vec<f32>, // rolling chop for z-score
    pub trend_buffer: Vec<f32>,// rolling trend for z-score
    pub period: usize,
    pub z_window: usize,
    pub pos: usize,
    pub count: usize,
}

impl CtiState {
    pub fn new(period: usize, z_window: usize) -> Self {
        Self {
            atr_1: AtrState::new(1),
            tr_buffer: vec![0.0; period],
            high_buffer: vec![0.0; period],
            low_buffer: vec![f32::MAX; period],
            close_buffer: vec![0.0; period + 1],
            chop_buffer: vec![0.0; z_window],
            trend_buffer: vec![0.0; z_window],
            period, z_window, pos: 0, count: 0,
        }
    }
    
    /// Update, returns (chop, trend, chop_z, trend_z)
    #[inline]
    pub fn update(&mut self, high: f32, low: f32, close: f32, prev_close: f32) -> (f32, f32, f32, f32) {
        // True Range
        let tr = (high - low).max((high - prev_close).abs()).max((low - prev_close).abs());
        
        // Store values
        self.tr_buffer[self.pos % self.period] = tr;
        self.high_buffer[self.pos % self.period] = high;
        self.low_buffer[self.pos % self.period] = low;
        self.close_buffer[self.pos % (self.period + 1)] = close;
        
        if self.count < self.period {
            self.count += 1;
            self.pos += 1;
            return (50.0, 0.5, 0.0, 0.0);
        }
        
        // Calculate Choppiness Index
        let atr_sum: f32 = self.tr_buffer.iter().sum();
        let hh = self.high_buffer.iter().cloned().fold(f32::MIN, f32::max);
        let ll = self.low_buffer.iter().cloned().fold(f32::MAX, f32::min);
        let range = hh - ll;
        
        let chop = if range > 1e-10 && atr_sum > 1e-10 {
            (100.0 * (atr_sum / range).ln() / (self.period as f32).ln()).clamp(0.0, 100.0)
        } else { 50.0 };
        
        // Calculate Trend Efficiency (Kaufman-style)
        let old_close_pos = (self.pos + 1) % (self.period + 1);
        let old_close = self.close_buffer[old_close_pos];
        let net_move = (close - old_close).abs();
        let gross_move: f32 = self.tr_buffer.iter().sum();
        let trend = if gross_move > 1e-10 { (net_move / gross_move).clamp(0.0, 1.0) } else { 0.0 };
        
        // Z-scores
        let z_pos = self.count % self.z_window;
        self.chop_buffer[z_pos] = chop;
        self.trend_buffer[z_pos] = trend;
        
        let (chop_z, trend_z) = if self.count >= self.z_window {
            let chop_mean: f32 = self.chop_buffer.iter().sum::<f32>() / self.z_window as f32;
            let chop_var: f32 = self.chop_buffer.iter().map(|x| (x - chop_mean).powi(2)).sum::<f32>() / self.z_window as f32;
            let chop_std = chop_var.sqrt();
            let cz = if chop_std > 1e-10 { (chop - chop_mean) / chop_std } else { 0.0 };
            
            let trend_mean: f32 = self.trend_buffer.iter().sum::<f32>() / self.z_window as f32;
            let trend_var: f32 = self.trend_buffer.iter().map(|x| (x - trend_mean).powi(2)).sum::<f32>() / self.z_window as f32;
            let trend_std = trend_var.sqrt();
            let tz = if trend_std > 1e-10 { (trend - trend_mean) / trend_std } else { 0.0 };
            
            (cz, tz)
        } else { (0.0, 0.0) };
        
        self.pos += 1;
        (chop, trend, chop_z, trend_z)
    }
}

impl Default for CtiState { fn default() -> Self { Self::new(14, 50) } }

// =============================================================================
// FAST INDICATOR CALCULATOR - All Indicators Combined
// =============================================================================

/// High-performance incremental indicator calculator.
/// Maintains state for all standard indicators (0-24).
pub struct FastIndicatorCalculator {
    // Standard indicators
    pub rsi_14: RsiState,
    pub sma_10: SmaState,
    pub sma_20: SmaState,
    pub sma_50: SmaState,
    pub ema_9: EmaState,
    pub ema_21: EmaState,
    pub ema_55: EmaState,
    pub macd: MacdState,
    pub bollinger: BollingerState,
    pub atr_14: AtrState,
    pub roc_10: RocState,
    pub mfi_14: MfiState,
    pub cci_20: CciState,
    pub adx_14: AdxState,
    // Premium indicators (19-24)
    pub fvg: FvgState,
    pub vwap: VwapState,
    pub cti: CtiState,
    
    // Track previous close for CTI
    prev_close: f32,
    
    // Last computed values (for quick access)
    last_values: [f32; 25],
    tick_count: u64,
}

impl FastIndicatorCalculator {
    pub fn new() -> Self {
        Self {
            rsi_14: RsiState::new(14),
            sma_10: SmaState::new(10),
            sma_20: SmaState::new(20),
            sma_50: SmaState::new(50),
            ema_9: EmaState::new(9),
            ema_21: EmaState::new(21),
            ema_55: EmaState::new(55),
            macd: MacdState::new(12, 26, 9),
            bollinger: BollingerState::new(20, 2.0),
            atr_14: AtrState::new(14),
            roc_10: RocState::new(10),
            mfi_14: MfiState::new(14),
            cci_20: CciState::new(20),
            adx_14: AdxState::new(14),
            fvg: FvgState::new(),
            vwap: VwapState::new(),
            cti: CtiState::new(14, 50),
            prev_close: 0.0,
            last_values: [0.0; 25],
            tick_count: 0,
        }
    }
    
    /// Update all indicators with new OHLCV bar.
    /// Returns reference to last values array for zero-copy access.
    #[inline]
    pub fn update(&mut self, _open: f32, high: f32, low: f32, close: f32, volume: f32) -> &[f32; 25] {
        // ID 0: RSI_14
        self.last_values[0] = self.rsi_14.update(close);
        
        // ID 1-3: SMA_10, SMA_20, SMA_50
        self.last_values[1] = self.sma_10.update(close);
        self.last_values[2] = self.sma_20.update(close);
        self.last_values[3] = self.sma_50.update(close);
        
        // ID 4-6: EMA_9, EMA_21, EMA_55
        self.last_values[4] = self.ema_9.update(close);
        self.last_values[5] = self.ema_21.update(close);
        self.last_values[6] = self.ema_55.update(close);
        
        // ID 7-9: MACD
        let (macd_line, signal, histogram) = self.macd.update(close);
        self.last_values[7] = macd_line;
        self.last_values[8] = signal;
        self.last_values[9] = histogram;
        
        // ID 10-13: Bollinger Bands
        let (bb_upper, bb_middle, bb_lower, bb_pos) = self.bollinger.update(close);
        self.last_values[10] = bb_pos;
        self.last_values[11] = bb_upper;
        self.last_values[12] = bb_middle;
        self.last_values[13] = bb_lower;
        
        // ID 14: ATR_14
        self.last_values[14] = self.atr_14.update(high, low, close);
        
        // ID 15: ROC_10
        self.last_values[15] = self.roc_10.update(close);
        
        // ID 16: MFI_14
        self.last_values[16] = self.mfi_14.update(high, low, close, volume);
        
        // ID 17: CCI_20
        self.last_values[17] = self.cci_20.update(high, low, close);
        
        // ID 18: ADX_14
        self.last_values[18] = self.adx_14.update(high, low, close);
        
        // ID 19: FVG (Fair Value Gap)
        self.last_values[19] = self.fvg.update(high, low);
        
        // ID 20: VWAP
        self.last_values[20] = self.vwap.update(high, low, close, volume);
        
        // ID 21-24: CTI (Choppiness, Trend, z-scores)
        let (chop, trend, chop_z, trend_z) = self.cti.update(high, low, close, self.prev_close);
        self.last_values[21] = chop;
        self.last_values[22] = trend;
        self.last_values[23] = chop_z;
        self.last_values[24] = trend_z;
        
        // Update prev_close for next iteration
        self.prev_close = close;
        
        self.tick_count += 1;
        &self.last_values
    }
    
    /// Update only the specified indicators (bitmask: bit N = indicator ID N).
    /// Use `update()` for all indicators, this for selective calculation.
    /// mask=0 means calculate ALL (legacy behavior).
    #[inline]
    pub fn update_selective(&mut self, _open: f32, high: f32, low: f32, close: f32, volume: f32, mask: u32) -> &[f32; 25] {
        // mask=0 means all indicators (backward compat)
        let all = mask == 0;
        
        // ID 0: RSI_14
        if all || (mask & (1 << 0)) != 0 {
            self.last_values[0] = self.rsi_14.update(close);
        }
        
        // ID 1-3: SMA_10, SMA_20, SMA_50
        if all || (mask & (1 << 1)) != 0 {
            self.last_values[1] = self.sma_10.update(close);
        }
        if all || (mask & (1 << 2)) != 0 {
            self.last_values[2] = self.sma_20.update(close);
        }
        if all || (mask & (1 << 3)) != 0 {
            self.last_values[3] = self.sma_50.update(close);
        }
        
        // ID 4-6: EMA_9, EMA_21, EMA_55
        if all || (mask & (1 << 4)) != 0 {
            self.last_values[4] = self.ema_9.update(close);
        }
        if all || (mask & (1 << 5)) != 0 {
            self.last_values[5] = self.ema_21.update(close);
        }
        if all || (mask & (1 << 6)) != 0 {
            self.last_values[6] = self.ema_55.update(close);
        }
        
        // ID 7-9: MACD (all three are computed together)
        // bits 7, 8, 9 = 0x380
        if all || (mask & 0x380) != 0 {
            let (macd_line, signal, histogram) = self.macd.update(close);
            self.last_values[7] = macd_line;
            self.last_values[8] = signal;
            self.last_values[9] = histogram;
        }
        
        // ID 10-13: Bollinger Bands (all four computed together)
        // bits 10, 11, 12, 13 = 0x3C00
        if all || (mask & 0x3C00) != 0 {
            let (bb_upper, bb_middle, bb_lower, bb_pos) = self.bollinger.update(close);
            self.last_values[10] = bb_pos;
            self.last_values[11] = bb_upper;
            self.last_values[12] = bb_middle;
            self.last_values[13] = bb_lower;
        }
        
        // ID 14: ATR_14
        if all || (mask & (1 << 14)) != 0 {
            self.last_values[14] = self.atr_14.update(high, low, close);
        }
        
        // ID 15: ROC_10
        if all || (mask & (1 << 15)) != 0 {
            self.last_values[15] = self.roc_10.update(close);
        }
        
        // ID 16: MFI_14
        if all || (mask & (1 << 16)) != 0 {
            self.last_values[16] = self.mfi_14.update(high, low, close, volume);
        }
        
        // ID 17: CCI_20
        if all || (mask & (1 << 17)) != 0 {
            self.last_values[17] = self.cci_20.update(high, low, close);
        }
        
        // ID 18: ADX_14
        if all || (mask & (1 << 18)) != 0 {
            self.last_values[18] = self.adx_14.update(high, low, close);
        }
        
        // ID 19: FVG
        if all || (mask & (1 << 19)) != 0 {
            self.last_values[19] = self.fvg.update(high, low);
        }
        
        // ID 20: VWAP
        if all || (mask & (1 << 20)) != 0 {
            self.last_values[20] = self.vwap.update(high, low, close, volume);
        }
        
        // ID 21-24: CTI (all four computed together)
        // bits 21, 22, 23, 24 = 0x1E00000
        if all || (mask & 0x1E00000) != 0 {
            let (chop, trend, chop_z, trend_z) = self.cti.update(high, low, close, self.prev_close);
            self.last_values[21] = chop;
            self.last_values[22] = trend;
            self.last_values[23] = chop_z;
            self.last_values[24] = trend_z;
        }
        
        // Update prev_close for CTI (always needed if CTI might be called)
        self.prev_close = close;
        
        self.tick_count += 1;
        &self.last_values
    }
    
    /// Get last computed value by index
    #[inline(always)]
    pub fn get(&self, idx: usize) -> f32 {
        if idx < 25 { self.last_values[idx] } else { 0.0 }
    }
    
    /// Get all last values as HashMap (for SignalContext compatibility)
    pub fn to_hashmap(&self) -> HashMap<usize, Vec<f32>> {
        let mut map = HashMap::new();
        for i in 0..25 {
            map.insert(i, vec![self.last_values[i]]);
        }
        map
    }
    
    /// Bulk initialize from historical data
    pub fn initialize_from_history(&mut self, ohlcv: &[(f32, f32, f32, f32, f32)]) {
        for &(o, h, l, c, v) in ohlcv {
            self.update(o, h, l, c, v);
        }
    }
    
    /// Get tick count
    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }
}

impl Default for FastIndicatorCalculator {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// SHARED INDICATOR CACHE - Thread-Safe with Arc
// =============================================================================

/// Thread-safe indicator cache for sharing between threads.
/// Writers update via `FastIndicatorCalculator`, readers get Arc snapshots.
pub struct SharedIndicatorCache {
    /// Full history of indicators (for strategies needing lookback)
    pub history: Arc<parking_lot::RwLock<IndicatorHistory>>,
    /// Latest single values (for fast read)
    pub latest: Arc<[AtomicU64; 25]>,
}

/// History buffer for indicator values
pub struct IndicatorHistory {
    pub values: [Vec<f32>; 25],
    pub max_history: usize,
}

impl IndicatorHistory {
    pub fn new(max_history: usize) -> Self {
        Self {
            values: Default::default(),
            max_history,
        }
    }
    
    pub fn push(&mut self, values: &[f32; 25]) {
        for (i, v) in values.iter().enumerate() {
            self.values[i].push(*v);
            if self.values[i].len() > self.max_history {
                self.values[i].remove(0);
            }
        }
    }
    
    pub fn get(&self, idx: usize) -> &[f32] {
        if idx < 25 { &self.values[idx] } else { &[] }
    }
    
    pub fn len(&self) -> usize {
        self.values[0].len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.values[0].is_empty()
    }
}

impl Default for IndicatorHistory {
    fn default() -> Self {
        Self::new(1000)
    }
}

impl SharedIndicatorCache {
    pub fn new(max_history: usize) -> Self {
        Self {
            history: Arc::new(parking_lot::RwLock::new(IndicatorHistory::new(max_history))),
            latest: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
        }
    }
    
    /// Update cache with new values
    pub fn update(&self, values: &[f32; 25]) {
        // Update atomic latest values
        for (i, &v) in values.iter().enumerate() {
            self.latest[i].store(v.to_bits() as u64, Ordering::Release);
        }
        
        // Update history
        self.history.write().push(values);
    }
    
    /// Get latest value by index (lock-free)
    #[inline(always)]
    pub fn get_latest(&self, idx: usize) -> f32 {
        if idx < 25 {
            f32::from_bits(self.latest[idx].load(Ordering::Acquire) as u32)
        } else {
            0.0
        }
    }
    
    /// Get history slice (requires read lock)
    pub fn get_history(&self, idx: usize) -> Vec<f32> {
        self.history.read().get(idx).to_vec()
    }
    
    /// Get all history as HashMap for SignalContext
    ///
    /// `mask` follows the same convention as `update_selective`:
    /// - `0` → include all 25 indicators (legacy "all" behavior)
    /// - non-zero → only include indicators whose bit is set
    ///
    /// Each included indicator clones its full ring buffer (`Vec<f32>` of up
    /// to `max_history` elements). On the live tick path this runs per
    /// symbol per new-bar tick and was the dominant signal-phase spike
    /// (~25 × ~4 KB allocations per call). Masking it down to the 3–5
    /// indicators a strategy actually reads cuts the cost by ~80–90%.
    pub fn to_signal_context_map(&self, mask: u32) -> Arc<HashMap<usize, Vec<f32>>> {
        let history = self.history.read();
        if mask == 0 {
            let mut map = HashMap::with_capacity(25);
            for i in 0..25 {
                map.insert(i, history.get(i).to_vec());
            }
            Arc::new(map)
        } else {
            let n = mask.count_ones() as usize;
            let mut map = HashMap::with_capacity(n);
            for i in 0..25 {
                if mask & (1u32 << i) != 0 {
                    map.insert(i, history.get(i).to_vec());
                }
            }
            Arc::new(map)
        }
    }
}

impl Default for SharedIndicatorCache {
    fn default() -> Self {
        Self::new(1000)
    }
}

// =============================================================================
// BATCH COMPUTATION FUNCTIONS (for backtesting/strategy calculate_custom_indicators)
// =============================================================================
//
// These functions compute indicators over entire price arrays. Arc variants
// accept Arc<Vec<f32>> for zero-copy integration with SignalContext.

use std::sync::Arc as StdArc;

/// Calculate SMA (Simple Moving Average) over array
#[inline]
pub fn calculate_sma_rust(prices: &[f32], period: usize) -> Vec<f32> {
    let n = prices.len();
    let mut sma = vec![f32::NAN; n];
    if n < period || period == 0 { return sma; }

    let mut sum: f32 = prices[0..period].iter().sum();
    sma[period - 1] = sum / period as f32;
    for i in period..n {
        sum = sum - prices[i - period] + prices[i];
        sma[i] = sum / period as f32;
    }
    sma
}

/// Arc variant — zero-copy input, returns Arc for cheap cloning
#[inline]
pub fn calculate_sma_arc(prices: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_sma_rust(prices, period))
}

/// Calculate EMA using Standard alpha = 2/(period+1)
#[inline]
pub fn calculate_ema_rust(prices: &[f32], period: usize) -> Vec<f32> {
    let n = prices.len();
    let mut ema = vec![f32::NAN; n];
    if n < period || period == 0 { return ema; }

    let alpha = 2.0 / (period + 1) as f32;
    let initial_sma: f32 = prices[0..period].iter().sum::<f32>() / period as f32;
    ema[period - 1] = initial_sma;

    for i in period..n {
        ema[i] = alpha * prices[i] + (1.0 - alpha) * ema[i - 1];
    }
    ema
}

/// Arc variant
#[inline]
pub fn calculate_ema_arc(prices: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_ema_rust(prices, period))
}

/// Calculate RSI using Wilder's Smoothing
#[inline]
pub fn calculate_rsi_rust(prices: &[f32], period: usize) -> Vec<f32> {
    let n = prices.len();
    let mut rsi = vec![f32::NAN; n];
    if n <= period || period == 0 { return rsi; }

    // Compute initial avg gain/loss without allocating separate gain/loss vecs.
    let pf = period as f32;
    let mut avg_gain = 0.0f32;
    let mut avg_loss = 0.0f32;
    for i in 1..=period {
        let d = prices[i] - prices[i - 1];
        if d > 0.0 { avg_gain += d; } else { avg_loss -= d; }
    }
    avg_gain /= pf;
    avg_loss /= pf;

    rsi[period] = if avg_loss == 0.0 { 100.0 } else { 100.0 - 100.0 / (1.0 + avg_gain / avg_loss) };

    let smooth = pf - 1.0;
    for i in period + 1..n {
        let d = prices[i] - prices[i - 1];
        let (g, l) = if d > 0.0 { (d, 0.0) } else { (0.0, -d) };
        avg_gain = (avg_gain * smooth + g) / pf;
        avg_loss = (avg_loss * smooth + l) / pf;
        rsi[i] = if avg_loss == 0.0 { 100.0 } else { 100.0 - 100.0 / (1.0 + avg_gain / avg_loss) };
    }
    rsi
}

/// Arc variant
#[inline]
pub fn calculate_rsi_arc(prices: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_rsi_rust(prices, period))
}

/// Calculate MACD — returns (macd_line, signal_line, histogram)
#[inline]
pub fn calculate_macd_rust(prices: &[f32], fast: usize, slow: usize, signal: usize) -> (Vec<f32>, Vec<f32>, Vec<f32>) {
    let n = prices.len();
    let mut macd = vec![f32::NAN; n];
    let mut sig = vec![f32::NAN; n];
    let mut histogram = vec![f32::NAN; n];
    if n < slow || slow == 0 { return (macd, sig, histogram); }

    let ema_fast = calculate_ema_rust(prices, fast);
    let ema_slow = calculate_ema_rust(prices, slow);

    for i in slow - 1..n {
        macd[i] = ema_fast[i] - ema_slow[i];
    }

    let macd_start_idx = slow - 1;
    if n > macd_start_idx + signal {
        let alpha = 2.0 / (signal + 1) as f32;
        let mut sum = 0.0;
        for i in 0..signal {
            sum += macd[macd_start_idx + i];
        }
        let initial_signal = sum / signal as f32;
        let signal_start_idx = macd_start_idx + signal - 1;
        sig[signal_start_idx] = initial_signal;

        for i in signal_start_idx + 1..n {
            sig[i] = alpha * macd[i] + (1.0 - alpha) * sig[i - 1];
        }
    }

    for i in 0..n {
        if !macd[i].is_nan() && !sig[i].is_nan() {
            histogram[i] = macd[i] - sig[i];
        }
    }
    (macd, sig, histogram)
}

/// Arc variant — returns tuple of Arcs
#[inline]
pub fn calculate_macd_arc(prices: &StdArc<Vec<f32>>, fast: usize, slow: usize, signal: usize) -> (StdArc<Vec<f32>>, StdArc<Vec<f32>>, StdArc<Vec<f32>>) {
    let (m, s, h) = calculate_macd_rust(prices, fast, slow, signal);
    (StdArc::new(m), StdArc::new(s), StdArc::new(h))
}

/// Calculate Bollinger Bands — returns (Upper, Middle, Lower)
#[inline]
pub fn calculate_bollinger_bands_rust(prices: &[f32], period: usize, std_mult: f32) -> (Vec<f32>, Vec<f32>, Vec<f32>) {
    let n = prices.len();
    let mut upper = vec![f32::NAN; n];
    let mut middle = vec![f32::NAN; n];
    let mut lower = vec![f32::NAN; n];
    if n < period || period == 0 { return (upper, middle, lower); }

    // O(n) sliding window: maintain running sum and sum-of-squares.
    // variance = E[x²] - E[x]²  →  std_dev = sqrt(max(variance, 0))
    // (max guards against tiny negative floats from cancellation)
    let pf = period as f32;
    let inv_p = 1.0 / pf;
    let mut sum:    f32 = prices[..period].iter().sum();
    let mut sum_sq: f32 = prices[..period].iter().map(|&x| x * x).sum();

    let emit = |i: usize, sum: f32, sum_sq: f32,
                upper: &mut Vec<f32>, middle: &mut Vec<f32>, lower: &mut Vec<f32>| {
        let mean    = sum * inv_p;
        let std_dev = ((sum_sq * inv_p - mean * mean).max(0.0)).sqrt();
        middle[i] = mean;
        upper[i]  = mean + std_mult * std_dev;
        lower[i]  = mean - std_mult * std_dev;
    };
    emit(period - 1, sum, sum_sq, &mut upper, &mut middle, &mut lower);

    for i in period..n {
        let old = prices[i - period];
        let new = prices[i];
        sum    = sum    - old         + new;
        sum_sq = sum_sq - old * old   + new * new;
        emit(i, sum, sum_sq, &mut upper, &mut middle, &mut lower);
    }
    (upper, middle, lower)
}

/// Arc variant
#[inline]
pub fn calculate_bollinger_bands_arc(prices: &StdArc<Vec<f32>>, period: usize, std_mult: f32) -> (StdArc<Vec<f32>>, StdArc<Vec<f32>>, StdArc<Vec<f32>>) {
    let (u, m, l) = calculate_bollinger_bands_rust(prices, period, std_mult);
    (StdArc::new(u), StdArc::new(m), StdArc::new(l))
}

/// Calculate ATR (Average True Range) using Wilder's Smoothing
#[inline]
pub fn calculate_atr_rust(high: &[f32], low: &[f32], close: &[f32], period: usize) -> Vec<f32> {
    let n = high.len();
    let mut atr = vec![f32::NAN; n];
    if n <= period || period == 0 { return atr; }

    let mut tr = vec![0.0f32; n];
    tr[0] = high[0] - low[0];
    for i in 1..n {
        let hl = high[i] - low[i];
        let hc = (high[i] - close[i - 1]).abs();
        let lc = (low[i] - close[i - 1]).abs();
        tr[i] = hl.max(hc).max(lc);
    }

    let mut current_atr: f32 = tr[0..period].iter().sum::<f32>() / period as f32;
    atr[period - 1] = current_atr;
    for i in period..n {
        current_atr = ((current_atr * (period as f32 - 1.0)) + tr[i]) / period as f32;
        atr[i] = current_atr;
    }
    atr
}

/// Arc variant
#[inline]
pub fn calculate_atr_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_atr_rust(high, low, close, period))
}

/// Bollinger Band position (0.0 = at lower, 1.0 = at upper)
#[inline]
pub fn bb_position(close: &[f32], upper: &[f32], lower: &[f32]) -> Vec<f32> {
    let n = close.len();
    let mut result = vec![f32::NAN; n];
    for i in 0..n {
        if !upper[i].is_nan() && !lower[i].is_nan() {
            let range = upper[i] - lower[i];
            result[i] = if range == 0.0 { 0.5 } else { (close[i] - lower[i]) / range };
        }
    }
    result
}

/// Arc variant
#[inline]
pub fn bb_position_arc(close: &StdArc<Vec<f32>>, upper: &StdArc<Vec<f32>>, lower: &StdArc<Vec<f32>>) -> StdArc<Vec<f32>> {
    StdArc::new(bb_position(close, upper, lower))
}

/// Stochastic Fast %K
#[inline]
pub fn stoch_k(high: &[f32], low: &[f32], close: &[f32], period: usize) -> Vec<f32> {
    let n = high.len();
    let mut result = vec![f32::NAN; n];
    if n < period || period == 0 { return result; }
    for i in (period - 1)..n {
        let hh = high[(i + 1 - period)..=i].iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let ll = low[(i + 1 - period)..=i].iter().copied().fold(f32::INFINITY, f32::min);
        let range = hh - ll;
        result[i] = if range == 0.0 { 50.0 } else { (close[i] - ll) / range * 100.0 };
    }
    result
}

/// Arc variant
#[inline]
pub fn stoch_k_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(stoch_k(high, low, close, period))
}

/// On-Balance Volume
#[inline]
pub fn obv(close: &[f32], volume: &[f32]) -> Vec<f32> {
    let n = close.len();
    let mut result = vec![0.0f32; n];
    for i in 1..n {
        if close[i] > close[i - 1] { result[i] = result[i - 1] + volume[i]; }
        else if close[i] < close[i - 1] { result[i] = result[i - 1] - volume[i]; }
        else { result[i] = result[i - 1]; }
    }
    result
}

/// Arc variant
#[inline]
pub fn obv_arc(close: &StdArc<Vec<f32>>, volume: &StdArc<Vec<f32>>) -> StdArc<Vec<f32>> {
    StdArc::new(obv(close, volume))
}

/// Chaikin Money Flow
#[inline]
pub fn cmf(high: &[f32], low: &[f32], close: &[f32], volume: &[f32], period: usize) -> Vec<f32> {
    let n = high.len();
    let mut result = vec![f32::NAN; n];
    if n < period || period == 0 { return result; }

    let mut mfv = vec![0.0f32; n];
    for i in 0..n {
        let hl = high[i] - low[i];
        let clv = if hl == 0.0 { 0.0 } else { ((close[i] - low[i]) - (high[i] - close[i])) / hl };
        mfv[i] = clv * volume[i];
    }

    for i in (period - 1)..n {
        let sum_mfv: f32 = mfv[(i + 1 - period)..=i].iter().sum();
        let sum_vol: f32 = volume[(i + 1 - period)..=i].iter().sum();
        result[i] = if sum_vol == 0.0 { 0.0 } else { sum_mfv / sum_vol };
    }
    result
}

/// Arc variant
#[inline]
pub fn cmf_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>, volume: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(cmf(high, low, close, volume, period))
}

/// Rolling standard deviation
#[inline]
pub fn rolling_std(prices: &[f32], period: usize) -> Vec<f32> {
    let n = prices.len();
    let mut result = vec![f32::NAN; n];
    if n < period || period == 0 { return result; }
    let mean = calculate_sma_rust(prices, period);
    for i in (period - 1)..n {
        if mean[i].is_nan() { continue; }
        let slice = &prices[(i + 1 - period)..=i];
        let var: f32 = slice.iter().map(|x| (x - mean[i]).powi(2)).sum::<f32>() / period as f32;
        result[i] = var.sqrt();
    }
    result
}

/// Arc variant
#[inline]
pub fn rolling_std_arc(prices: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(rolling_std(prices, period))
}

/// Rolling max over a window
#[inline]
pub fn rolling_max(prices: &[f32], period: usize) -> Vec<f32> {
    let n = prices.len();
    let mut result = vec![f32::NAN; n];
    if n < period || period == 0 { return result; }
    for i in (period - 1)..n {
        result[i] = prices[(i + 1 - period)..=i].iter().copied().fold(f32::NEG_INFINITY, f32::max);
    }
    result
}

/// Arc variant
#[inline]
pub fn rolling_max_arc(prices: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(rolling_max(prices, period))
}

/// Rolling min over a window
#[inline]
pub fn rolling_min(prices: &[f32], period: usize) -> Vec<f32> {
    let n = prices.len();
    let mut result = vec![f32::NAN; n];
    if n < period || period == 0 { return result; }
    for i in (period - 1)..n {
        result[i] = prices[(i + 1 - period)..=i].iter().copied().fold(f32::INFINITY, f32::min);
    }
    result
}

/// Arc variant
#[inline]
pub fn rolling_min_arc(prices: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(rolling_min(prices, period))
}

/// Typical price
#[inline]
pub fn typical_price(high: &[f32], low: &[f32], close: &[f32]) -> Vec<f32> {
    high.iter().zip(low).zip(close).map(|((h, l), c)| (h + l + c) / 3.0).collect()
}

/// Arc variant
#[inline]
pub fn typical_price_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>) -> StdArc<Vec<f32>> {
    StdArc::new(typical_price(high, low, close))
}

/// Keltner Channel — returns (middle, upper, lower)
#[inline]
pub fn keltner_channel(
    close: &[f32], high: &[f32], low: &[f32],
    ema_period: usize, atr_period: usize, mult: f32,
) -> (Vec<f32>, Vec<f32>, Vec<f32>) {
    let n = close.len();
    let mid = calculate_ema_rust(close, ema_period);
    let a = calculate_atr_rust(high, low, close, atr_period);
    let mut upper = vec![f32::NAN; n];
    let mut lower = vec![f32::NAN; n];
    for i in 0..n {
        if !mid[i].is_nan() && !a[i].is_nan() {
            upper[i] = mid[i] + a[i] * mult;
            lower[i] = mid[i] - a[i] * mult;
        }
    }
    (mid, upper, lower)
}

/// Arc variant
#[inline]
pub fn keltner_channel_arc(
    close: &StdArc<Vec<f32>>, high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>,
    ema_period: usize, atr_period: usize, mult: f32,
) -> (StdArc<Vec<f32>>, StdArc<Vec<f32>>, StdArc<Vec<f32>>) {
    let (m, u, l) = keltner_channel(close, high, low, ema_period, atr_period, mult);
    (StdArc::new(m), StdArc::new(u), StdArc::new(l))
}

/// Fibonacci retracement levels from rolling high/low
#[inline]
pub fn fibonacci_levels(high: &[f32], low: &[f32], lookback: usize) -> (Vec<f32>, Vec<f32>) {
    let n = high.len();
    let mut fib_382 = vec![f32::NAN; n];
    let mut fib_618 = vec![f32::NAN; n];
    if n < lookback || lookback == 0 { return (fib_382, fib_618); }
    for i in (lookback - 1)..n {
        let hh = high[(i + 1 - lookback)..=i].iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let ll = low[(i + 1 - lookback)..=i].iter().copied().fold(f32::INFINITY, f32::min);
        let range = hh - ll;
        fib_382[i] = ll + range * 0.382;
        fib_618[i] = ll + range * 0.618;
    }
    (fib_382, fib_618)
}

/// Arc variant
#[inline]
pub fn fibonacci_levels_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, lookback: usize) -> (StdArc<Vec<f32>>, StdArc<Vec<f32>>) {
    let (f382, f618) = fibonacci_levels(high, low, lookback);
    (StdArc::new(f382), StdArc::new(f618))
}

// =============================================================================
// ADDITIONAL INDICATORS (migrated from indicators.rs)
// =============================================================================

/// Calculate ROC (Rate of Change) — ((Price/PrevPrice) - 1) * 100
#[inline]
pub fn calculate_roc_rust(prices: &[f32], period: usize) -> Vec<f32> {
    let n = prices.len();
    let mut roc = vec![f32::NAN; n];
    if n <= period || period == 0 { return roc; }

    for i in period..n {
        let base = prices[i - period];
        // branchless: prices are always > 0 for financial data; max(ε) avoids div-by-zero
        roc[i] = (prices[i] / base.max(f32::EPSILON) - 1.0) * 100.0;
    }
    roc
}

/// Arc variant
#[inline]
pub fn calculate_roc_arc(prices: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_roc_rust(prices, period))
}

/// Calculate MFI (Money Flow Index)
#[inline]
pub fn calculate_mfi_rust(high: &[f32], low: &[f32], close: &[f32], volume: &[f32], period: usize) -> Vec<f32> {
    let n = high.len();
    let mut mfi = vec![f32::NAN; n];
    if n <= period || period == 0 { return mfi; }

    let mut typical_price = vec![0.0f32; n];
    let mut raw_money_flow = vec![0.0f32; n];
    for i in 0..n {
        typical_price[i] = (high[i] + low[i] + close[i]) / 3.0;
        raw_money_flow[i] = typical_price[i] * volume[i];
    }

    let mut pos_flow_sum = 0.0;
    let mut neg_flow_sum = 0.0;
    for i in 1..=period {
        if typical_price[i] > typical_price[i - 1] {
            pos_flow_sum += raw_money_flow[i];
        } else if typical_price[i] < typical_price[i - 1] {
            neg_flow_sum += raw_money_flow[i];
        }
    }

    mfi[period] = if neg_flow_sum == 0.0 { 100.0 } else {
        let mr = pos_flow_sum / neg_flow_sum;
        100.0 - (100.0 / (1.0 + mr))
    };

    for i in period + 1..n {
        if typical_price[i - period] > typical_price[i - period - 1] {
            pos_flow_sum -= raw_money_flow[i - period];
        } else if typical_price[i - period] < typical_price[i - period - 1] {
            neg_flow_sum -= raw_money_flow[i - period];
        }

        if typical_price[i] > typical_price[i - 1] {
            pos_flow_sum += raw_money_flow[i];
        } else if typical_price[i] < typical_price[i - 1] {
            neg_flow_sum += raw_money_flow[i];
        }

        pos_flow_sum = pos_flow_sum.max(0.0);
        neg_flow_sum = neg_flow_sum.max(0.0);

        mfi[i] = if neg_flow_sum == 0.0 { 100.0 } else {
            let mr = pos_flow_sum / neg_flow_sum;
            100.0 - (100.0 / (1.0 + mr))
        };
    }
    mfi
}

/// Arc variant
#[inline]
pub fn calculate_mfi_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>, volume: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_mfi_rust(high, low, close, volume, period))
}

/// Calculate CCI (Commodity Channel Index)
#[inline]
pub fn calculate_cci_rust(high: &[f32], low: &[f32], close: &[f32], period: usize) -> Vec<f32> {
    let n = high.len();
    let mut cci = vec![f32::NAN; n];
    if n < period || period == 0 { return cci; }

    let mut tp = vec![0.0f32; n];
    for i in 0..n { tp[i] = (high[i] + low[i] + close[i]) / 3.0; }

    let mut sma_tp = vec![f32::NAN; n];
    let mut sum: f32 = tp[0..period].iter().sum();
    sma_tp[period - 1] = sum / period as f32;
    for i in period..n {
        sum = sum - tp[i - period] + tp[i];
        sma_tp[i] = sum / period as f32;
    }

    for i in period - 1..n {
        let mut mean_dev_sum = 0.0;
        let ma = sma_tp[i];
        for j in (i + 1 - period)..=i {
            mean_dev_sum += (tp[j] - ma).abs();
        }
        let mean_dev = mean_dev_sum / period as f32;
        cci[i] = if mean_dev != 0.0 { (tp[i] - ma) / (0.015 * mean_dev) } else { 0.0 };
    }
    cci
}

/// Arc variant
#[inline]
pub fn calculate_cci_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_cci_rust(high, low, close, period))
}

/// Calculate ADX using Wilder's Smoothing
#[inline]
pub fn calculate_adx_rust(high: &[f32], low: &[f32], close: &[f32], period: usize) -> Vec<f32> {
    let n = high.len();
    let mut adx = vec![f32::NAN; n];
    if n <= period * 2 || period == 0 { return adx; }

    let mut tr = vec![0.0f32; n];
    let mut dm_plus = vec![0.0f32; n];
    let mut dm_minus = vec![0.0f32; n];

    tr[0] = high[0] - low[0];
    for i in 1..n {
        let hl = high[i] - low[i];
        let hc = (high[i] - close[i - 1]).abs();
        let lc = (low[i] - close[i - 1]).abs();
        tr[i] = hl.max(hc).max(lc);

        let up_move = high[i] - high[i - 1];
        let down_move = low[i - 1] - low[i];
        if up_move > down_move && up_move > 0.0 { dm_plus[i] = up_move; }
        if down_move > up_move && down_move > 0.0 { dm_minus[i] = down_move; }
    }

    let mut tr_smooth = vec![0.0f32; n];
    let mut dm_plus_smooth = vec![0.0f32; n];
    let mut dm_minus_smooth = vec![0.0f32; n];

    tr_smooth[period] = tr[1..=period].iter().sum::<f32>();
    dm_plus_smooth[period] = dm_plus[1..=period].iter().sum();
    dm_minus_smooth[period] = dm_minus[1..=period].iter().sum();

    let mut dx = vec![f32::NAN; n];
    {
        let i = period;
        let di_plus = if tr_smooth[i] != 0.0 { 100.0 * dm_plus_smooth[i] / tr_smooth[i] } else { 0.0 };
        let di_minus = if tr_smooth[i] != 0.0 { 100.0 * dm_minus_smooth[i] / tr_smooth[i] } else { 0.0 };
        let sum_di = di_plus + di_minus;
        dx[i] = if sum_di != 0.0 { 100.0 * (di_plus - di_minus).abs() / sum_di } else { 0.0 };
    }

    for i in period + 1..n {
        tr_smooth[i] = tr_smooth[i - 1] - (tr_smooth[i - 1] / period as f32) + tr[i];
        dm_plus_smooth[i] = dm_plus_smooth[i - 1] - (dm_plus_smooth[i - 1] / period as f32) + dm_plus[i];
        dm_minus_smooth[i] = dm_minus_smooth[i - 1] - (dm_minus_smooth[i - 1] / period as f32) + dm_minus[i];

        let di_plus = if tr_smooth[i] != 0.0 { 100.0 * dm_plus_smooth[i] / tr_smooth[i] } else { 0.0 };
        let di_minus = if tr_smooth[i] != 0.0 { 100.0 * dm_minus_smooth[i] / tr_smooth[i] } else { 0.0 };
        let sum_di = di_plus + di_minus;
        dx[i] = if sum_di != 0.0 { 100.0 * (di_plus - di_minus).abs() / sum_di } else { 0.0 };
    }

    let adx_start = period * 2 - 1;
    if n > adx_start {
        let mut sum_dx = 0.0;
        for i in period..=adx_start { sum_dx += dx[i]; }
        adx[adx_start] = sum_dx / period as f32;
        for i in adx_start + 1..n {
            adx[i] = (adx[i - 1] * (period as f32 - 1.0) + dx[i]) / period as f32;
        }
    }
    adx
}

/// Arc variant
#[inline]
pub fn calculate_adx_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_adx_rust(high, low, close, period))
}

/// Calculate FVG (Fair Value Gap) as ratio
#[inline]
pub fn calculate_fvg_rust(high: &[f32], low: &[f32], close: &[f32], open: &[f32]) -> Vec<f32> {
    let n = high.len();
    let mut fvg = vec![0.0f32; n];
    if n < 3 { return fvg; }

    for i in 2..n {
        let up_gap = low[i] - high[i - 2];
        let down_gap = low[i - 2] - high[i];
        let body_size = (close[i - 1] - open[i - 1]).abs();
        let current_price = close[i];

        if up_gap > body_size * 0.5 && current_price != 0.0 {
            fvg[i] = up_gap / current_price;
        } else if down_gap > body_size * 0.5 && current_price != 0.0 {
            fvg[i] = -down_gap / current_price;
        }
    }
    fvg
}

/// Arc variant
#[inline]
pub fn calculate_fvg_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>, open: &StdArc<Vec<f32>>) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_fvg_rust(high, low, close, open))
}

/// Calculate VWAP (Volume Weighted Average Price)
#[inline]
pub fn calculate_vwap_rust(high: &[f32], low: &[f32], close: &[f32], volume: &[f32]) -> Vec<f32> {
    let n = high.len();
    let mut vwap = vec![f32::NAN; n];
    if n == 0 { return vwap; }

    let mut cum_vol = 0.0;
    let mut cum_pv = 0.0;
    for i in 0..n {
        let tp = (high[i] + low[i] + close[i]) / 3.0;
        cum_vol += volume[i];
        cum_pv += tp * volume[i];
        if cum_vol != 0.0 { vwap[i] = cum_pv / cum_vol; }
    }
    vwap
}

/// Arc variant
#[inline]
pub fn calculate_vwap_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>, volume: &StdArc<Vec<f32>>) -> StdArc<Vec<f32>> {
    StdArc::new(calculate_vwap_rust(high, low, close, volume))
}

/// Fair Value Gap detection (simplified) — 1.0 bullish, -1.0 bearish, 0.0 none
#[inline]
pub fn fvg_simple(high: &[f32], low: &[f32]) -> Vec<f32> {
    let n = high.len();
    let mut result = vec![0.0f32; n];
    if n < 3 { return result; }
    for i in 2..n {
        let bull = (low[i] > high[i - 2]) as u32 as f32;
        let bear = (high[i] < low[i - 2]) as u32 as f32;
        result[i] = bull - bear;
    }
    result
}

/// Arc variant
#[inline]
pub fn fvg_simple_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>) -> StdArc<Vec<f32>> {
    StdArc::new(fvg_simple(high, low))
}

/// Choppiness Index
#[inline]
pub fn choppiness_index(high: &[f32], low: &[f32], close: &[f32], period: usize) -> Vec<f32> {
    let n = high.len();
    let mut result = vec![f32::NAN; n];
    if n <= period || period == 0 { return result; }

    // Inline true range (= ATR period=1) — avoids separate Vec allocation + function call
    let mut tr = vec![0.0f32; n];
    tr[0] = high[0] - low[0];
    for i in 1..n {
        let hl = high[i] - low[i];
        let hc = (high[i] - close[i - 1]).abs();
        let lc = (low[i] - close[i - 1]).abs();
        tr[i] = hl.max(hc).max(lc);
    }

    // Monotone deques for O(n) rolling max (HH) and min (LL)
    // Each index enters and leaves the deque exactly once → O(n) total
    let mut hh_deque: std::collections::VecDeque<usize> = std::collections::VecDeque::with_capacity(period + 1);
    let mut ll_deque: std::collections::VecDeque<usize> = std::collections::VecDeque::with_capacity(period + 1);

    // Seed deques + TR sum with window [0, period-1]
    let mut tr_sum = 0.0f32;
    for i in 0..period {
        while !hh_deque.is_empty() && high[*hh_deque.back().unwrap()] <= high[i] { hh_deque.pop_back(); }
        while !ll_deque.is_empty() && low[*ll_deque.back().unwrap()]  >= low[i]  { ll_deque.pop_back(); }
        hh_deque.push_back(i);
        ll_deque.push_back(i);
        tr_sum += tr[i];
    }

    for i in period..n {
        let window_start = i + 1 - period;
        // Slide: evict oldest, add newest
        tr_sum = tr_sum - tr[i - period] + tr[i];
        while !hh_deque.is_empty() && *hh_deque.front().unwrap() < window_start { hh_deque.pop_front(); }
        while !ll_deque.is_empty() && *ll_deque.front().unwrap() < window_start { ll_deque.pop_front(); }
        while !hh_deque.is_empty() && high[*hh_deque.back().unwrap()] <= high[i] { hh_deque.pop_back(); }
        while !ll_deque.is_empty() && low[*ll_deque.back().unwrap()]  >= low[i]  { ll_deque.pop_back(); }
        hh_deque.push_back(i);
        ll_deque.push_back(i);

        let range = high[*hh_deque.front().unwrap()] - low[*ll_deque.front().unwrap()];
        if range > 0.0 {
            let ratio = tr_sum / range;
            if ratio > 0.0 {
                result[i] = 100.0 * ratio.ln() / (period as f32).ln();
            }
        }
    }
    result
}

/// Arc variant
#[inline]
pub fn choppiness_index_arc(high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, close: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(choppiness_index(high, low, close, period))
}

/// Trend efficiency ratio (Kaufman)
#[inline]
pub fn trend_efficiency(close: &[f32], period: usize) -> Vec<f32> {
    let n = close.len();
    let mut result = vec![f32::NAN; n];
    if n <= period || period == 0 { return result; }

    // Precompute per-bar absolute returns (O(n)), then use running window sum (O(1)/bar)
    let mut abs_ret = vec![0.0f32; n];
    for i in 1..n { abs_ret[i] = (close[i] - close[i - 1]).abs(); }

    let mut vol_sum: f32 = abs_ret[1..=period].iter().sum();
    result[period] = {
        let direction = (close[period] - close[0]).abs();
        if vol_sum == 0.0 { 0.0 } else { direction / vol_sum }
    };
    for i in period + 1..n {
        vol_sum = vol_sum - abs_ret[i - period] + abs_ret[i];
        let direction = (close[i] - close[i - period]).abs();
        result[i] = if vol_sum == 0.0 { 0.0 } else { direction / vol_sum };
    }
    result
}

/// Arc variant
#[inline]
pub fn trend_efficiency_arc(close: &StdArc<Vec<f32>>, period: usize) -> StdArc<Vec<f32>> {
    StdArc::new(trend_efficiency(close, period))
}

/// Calculate CTI (Choppiness/Trend Index with Z-scores)
#[inline]
pub fn calculate_cti_rust(
    high: &[f32],
    low: &[f32],
    close: &[f32],
    n: usize,
    z_window: usize,
) -> (Vec<f32>, Vec<f32>, Vec<f32>, Vec<f32>) {
    let len = close.len();
    let mut chop = vec![f32::NAN; len];
    let mut trend = vec![f32::NAN; len];
    let mut chop_z = vec![f32::NAN; len];
    let mut trend_z = vec![f32::NAN; len];

    if len < n + 1 || n == 0 {
        return (chop, trend, chop_z, trend_z);
    }

    let mut tr = vec![0.0f32; len];
    for i in 1..len {
        let hl = high[i] - low[i];
        let hc = (high[i] - close[i - 1]).abs();
        let lc = (low[i] - close[i - 1]).abs();
        tr[i] = hl.max(hc).max(lc);
    }

    let mut abs_diff = vec![0.0f32; len];
    for i in 1..len {
        abs_diff[i] = (close[i] - close[i - 1]).abs();
    }

    let log10_n = (n as f32).log10();

    for i in n..len {
        let atr_sum: f32 = tr[i - n + 1..=i].iter().sum();
        let mut highest = f32::MIN;
        let mut lowest = f32::MAX;
        for j in (i - n + 1)..=i {
            if high[j] > highest { highest = high[j]; }
            if low[j] < lowest { lowest = low[j]; }
        }
        let price_range = highest - lowest;

        if price_range > 1e-10 && atr_sum > 1e-10 {
            chop[i] = 100.0 * (atr_sum / price_range).log10() / log10_n;
            chop[i] = chop[i].max(0.0).min(100.0);
        } else {
            chop[i] = 50.0;
        }

        let net_move = (close[i] - close[i - n]).abs();
        let gross_move: f32 = abs_diff[i - n + 1..=i].iter().sum();

        if gross_move > 1e-10 {
            trend[i] = net_move / gross_move;
            trend[i] = trend[i].max(0.0).min(1.0);
        } else {
            trend[i] = 0.0;
        }
    }

    let min_samples = n.max(20);

    for i in (n + min_samples)..len {
        let window_start = if i >= z_window { i - z_window } else { n };
        let window_len = i - window_start;

        if window_len >= min_samples {
            let chop_slice: Vec<f32> = (window_start..i)
                .filter_map(|j| if chop[j].is_nan() { None } else { Some(chop[j]) })
                .collect();

            if chop_slice.len() >= min_samples {
                let chop_mean: f32 = chop_slice.iter().sum::<f32>() / chop_slice.len() as f32;
                let chop_var: f32 = chop_slice.iter()
                    .map(|x| (x - chop_mean).powi(2))
                    .sum::<f32>() / chop_slice.len() as f32;
                let chop_std = chop_var.sqrt();

                if chop_std > 1e-10 && !chop[i].is_nan() {
                    chop_z[i] = (chop[i] - chop_mean) / chop_std;
                }
            }

            let trend_slice: Vec<f32> = (window_start..i)
                .filter_map(|j| if trend[j].is_nan() { None } else { Some(trend[j]) })
                .collect();

            if trend_slice.len() >= min_samples {
                let trend_mean: f32 = trend_slice.iter().sum::<f32>() / trend_slice.len() as f32;
                let trend_var: f32 = trend_slice.iter()
                    .map(|x| (x - trend_mean).powi(2))
                    .sum::<f32>() / trend_slice.len() as f32;
                let trend_std = trend_var.sqrt();

                if trend_std > 1e-10 && !trend[i].is_nan() {
                    trend_z[i] = (trend[i] - trend_mean) / trend_std;
                }
            }
        }
    }

    (chop, trend, chop_z, trend_z)
}

// =============================================================================
// ORDER FLOW INDICATORS (TBBO Data)
// =============================================================================

/// Delta EMA for order flow
#[inline]
pub fn calculate_delta_ema_rust(delta: &[f32], period: usize) -> Vec<f32> {
    let n = delta.len();
    let mut ema = vec![0.0f32; n];
    if n == 0 || period == 0 { return ema; }

    let alpha = 2.0 / (period + 1) as f32;
    ema[0] = delta[0];
    for i in 1..n {
        ema[i] = alpha * delta[i] + (1.0 - alpha) * ema[i - 1];
    }
    ema
}

/// Cumulative Volume Delta
#[inline]
pub fn calculate_cvd_rust(delta: &[f32]) -> Vec<f32> {
    let n = delta.len();
    let mut cvd = vec![0.0f32; n];
    if n == 0 { return cvd; }

    cvd[0] = delta[0];
    for i in 1..n {
        cvd[i] = cvd[i - 1] + delta[i];
    }
    cvd
}

/// CVD Divergence from price
#[inline]
pub fn calculate_cvd_divergence_rust(cvd: &[f32], close: &[f32], period: usize) -> Vec<f32> {
    let n = cvd.len();
    let mut divergence = vec![0.0f32; n];
    if n <= period || period == 0 { return divergence; }

    for i in period..n {
        let cvd_change = if cvd[i - period].abs() > 1e-10 {
            (cvd[i] - cvd[i - period]) / cvd[i - period].abs()
        } else { 0.0 };

        let price_change = if close[i - period] > 1e-10 {
            (close[i] - close[i - period]) / close[i - period]
        } else { 0.0 };

        divergence[i] = cvd_change - price_change;
    }
    divergence
}

/// Imbalance intensity weighted by volume
#[inline]
pub fn calculate_imbalance_intensity_rust(imbalance: &[f32], volume: &[f32], period: usize) -> Vec<f32> {
    let n = imbalance.len();
    let mut intensity = vec![0.0f32; n];
    if n < period || period == 0 { return intensity; }

    let mut vol_ma = vec![0.0f32; n];
    for i in period - 1..n {
        let sum: f32 = volume[i - period + 1..=i].iter().sum();
        vol_ma[i] = sum / period as f32;
    }

    for i in period - 1..n {
        let centered_imbalance = (imbalance[i] - 0.5) * 2.0;
        let vol_weight = if vol_ma[i] > 1e-10 {
            (volume[i] / vol_ma[i]).min(3.0)
        } else { 1.0 };
        intensity[i] = centered_imbalance * vol_weight;
    }
    intensity
}

/// Absorption detection
#[inline]
pub fn calculate_absorption_rust(delta: &[f32], close: &[f32], period: usize) -> Vec<f32> {
    let n = delta.len();
    let mut absorption = vec![0.0f32; n];
    if n <= period || period == 0 { return absorption; }

    let delta_sum: f32 = delta.iter().map(|x| x.abs()).sum();
    let delta_std = if n > 0 { delta_sum / n as f32 } else { 1.0 };

    let mut price_change = vec![0.0f32; n];
    for i in 1..n {
        if close[i - 1] > 1e-10 {
            price_change[i] = (close[i] - close[i - 1]) / close[i - 1];
        }
    }

    let price_std = {
        let sum: f32 = price_change.iter().map(|x| x.abs()).sum();
        if n > 0 { sum / n as f32 } else { 1.0 }
    };

    for i in period..n {
        let delta_window: f32 = delta[i - period..i].iter().map(|x| x.abs()).sum();
        let price_window: f32 = price_change[i - period..i].iter().map(|x| x.abs()).sum();

        let delta_norm = delta_window / (delta_std + 1e-10);
        let price_norm = price_window / (price_std + 1e-10);

        if delta_norm > 0.5 {
            absorption[i] = (delta_norm / (price_norm + 1e-10)).min(10.0);
        }
    }
    absorption
}

/// Order imbalance from bid/ask counts
#[inline]
pub fn calculate_order_imbalance_rust(bid_ct: &[f32], ask_ct: &[f32]) -> Vec<f32> {
    let n = bid_ct.len();
    let mut imbalance = vec![0.5f32; n];
    for i in 0..n {
        let total = bid_ct[i] + ask_ct[i];
        if total > 0.0 { imbalance[i] = bid_ct[i] / total; }
    }
    imbalance
}

/// Size imbalance from bid/ask sizes
#[inline]
pub fn calculate_size_imbalance_rust(bid_sz: &[f32], ask_sz: &[f32]) -> Vec<f32> {
    let n = bid_sz.len();
    let mut imbalance = vec![0.5f32; n];
    for i in 0..n {
        let total = bid_sz[i] + ask_sz[i];
        if total > 0.0 { imbalance[i] = bid_sz[i] / total; }
    }
    imbalance
}

/// Spread percentage from bid/ask prices
#[inline]
pub fn calculate_spread_pct_rust(bid_px: &[f32], ask_px: &[f32]) -> Vec<f32> {
    let n = bid_px.len();
    let mut spread_pct = vec![0.0f32; n];
    for i in 0..n {
        let mid = (bid_px[i] + ask_px[i]) / 2.0;
        if mid > 1e-10 {
            spread_pct[i] = (ask_px[i] - bid_px[i]) / mid;
        }
    }
    spread_pct
}

// =============================================================================
// COMPOSITE: calculate all standard indicators keyed by index 0-22
// =============================================================================

/// Calculate the 21 standard indicators as HashMap<usize, Vec<f32>> matching the index scheme:
/// 0:RSI_14  1:SMA_10  2:SMA_20  3:SMA_50  4:EMA_9  5:EMA_21  6:EMA_55
/// 7:MACD  8:MACD_SIGNAL  9:MACD_HIST  10:BB_POS  11:BB_UPPER  12:BB_MIDDLE
/// 13:BB_LOWER  14:ATR_14  15:ROC_10  16:MFI_14  17:CCI_20  18:ADX_14
/// 19:FVG  20:VWAP  21:CHOP  22:TREND_EFF
pub fn calculate_standard_indicators(
    close: &StdArc<Vec<f32>>, high: &StdArc<Vec<f32>>, low: &StdArc<Vec<f32>>, volume: &StdArc<Vec<f32>>,
) -> HashMap<usize, Vec<f32>> {
    // The 23 indicators are fully independent — split into 4 groups and run them in
    // parallel. Arc clones are O(1) (atomic ref-count bump only).
    let (ca, cb, cc, cd) = (close.clone(), close.clone(), close.clone(), close.clone());
    let (hc, hd)         = (high.clone(), high.clone());
    let (lc, ld)         = (low.clone(), low.clone());
    let (vc, vd)         = (volume.clone(), volume.clone());

    // 4-way parallel tree via nested rayon::join.
    let ((ga, gb), (gc, gd)) = rayon::join(
        || rayon::join(
            || {
                // Group A — close-only EMA/SMA/RSI (7 indicators)
                let rsi   = calculate_rsi_rust(&ca, 14);
                let sma10 = calculate_sma_rust(&ca, 10);
                let sma20 = calculate_sma_rust(&ca, 20);
                let sma50 = calculate_sma_rust(&ca, 50);
                let ema9  = calculate_ema_rust(&ca, 9);
                let ema21 = calculate_ema_rust(&ca, 21);
                let ema55 = calculate_ema_rust(&ca, 55);
                (rsi, sma10, sma20, sma50, ema9, ema21, ema55)
            },
            || {
                // Group B — MACD + Bollinger Bands (6 indicators)
                let (ml, sl, hl)         = calculate_macd_rust(&cb, 12, 26, 9);
                let (bb_u, bb_m, bb_l)   = calculate_bollinger_bands_rust(&cb, 20, 2.0);
                let bb_pos               = bb_position(&cb, &bb_u, &bb_l);
                (ml, sl, hl, bb_u, bb_m, bb_l, bb_pos)
            },
        ),
        || rayon::join(
            || {
                // Group C — multi-input: ATR, ROC, MFI, CCI (4 indicators)
                let atr = calculate_atr_rust(&hc, &lc, &cc, 14);
                let roc = calculate_roc_rust(&cc, 10);
                let mfi = calculate_mfi_rust(&hc, &lc, &cc, &vc, 14);
                let cci = calculate_cci_rust(&hc, &lc, &cc, 20);
                (atr, roc, mfi, cci)
            },
            || {
                // Group D — ADX, FVG, VWAP, CHOP, TREND (6 indicators)
                let adx   = calculate_adx_rust(&hd, &ld, &cd, 14);
                let fvg   = fvg_simple(&hd, &ld);
                let vwap  = calculate_vwap_rust(&hd, &ld, &cd, &vd);
                let chop  = choppiness_index(&hd, &ld, &cd, 14);
                let trend = trend_efficiency(&cd, 10);
                (adx, fvg, vwap, chop, trend)
            },
        ),
    );

    let (rsi, sma10, sma20, sma50, ema9, ema21, ema55) = ga;
    let (ml, sl, hl, bb_u, bb_m, bb_l, bb_pos)        = gb;
    let (atr, roc, mfi, cci)                           = gc;
    let (adx, fvg, vwap, chop, trend)                  = gd;

    let mut m = HashMap::with_capacity(23);
    m.insert(0,  rsi);
    m.insert(1,  sma10);
    m.insert(2,  sma20);
    m.insert(3,  sma50);
    m.insert(4,  ema9);
    m.insert(5,  ema21);
    m.insert(6,  ema55);
    m.insert(7,  ml);
    m.insert(8,  sl);
    m.insert(9,  hl);
    m.insert(10, bb_pos);
    m.insert(11, bb_u);
    m.insert(12, bb_m);
    m.insert(13, bb_l);
    m.insert(14, atr);
    m.insert(15, roc);
    m.insert(16, mfi);
    m.insert(17, cci);
    m.insert(18, adx);
    m.insert(19, fvg);
    m.insert(20, vwap);
    m.insert(21, chop);
    m.insert(22, trend);
    m
}

/// Calculate all standard indicators as a HashMap (legacy string-keyed version)
pub fn calculate_all(
    close: &[f32], high: &[f32], low: &[f32], _volume: &[f32],
) -> HashMap<String, Vec<f32>> {
    let mut result = HashMap::new();

    result.insert("rsi_14".into(), calculate_rsi_rust(close, 14));
    result.insert("ema_9".into(), calculate_ema_rust(close, 9));
    result.insert("ema_21".into(), calculate_ema_rust(close, 21));
    result.insert("sma_50".into(), calculate_sma_rust(close, 50));
    result.insert("sma_200".into(), calculate_sma_rust(close, 200));
    result.insert("atr_14".into(), calculate_atr_rust(high, low, close, 14));

    let (macd_line, macd_sig, macd_hist) = calculate_macd_rust(close, 12, 26, 9);
    result.insert("macd".into(), macd_line);
    result.insert("macd_signal".into(), macd_sig);
    result.insert("macd_hist".into(), macd_hist);

    let (bb_upper, bb_middle, bb_lower) = calculate_bollinger_bands_rust(close, 20, 2.0);
    result.insert("bb_upper".into(), bb_upper);
    result.insert("bb_middle".into(), bb_middle);
    result.insert("bb_lower".into(), bb_lower);

    result
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ring_buffer_basic() {
        let mut buf = OhlcvRingBuffer::new();
        assert!(buf.is_empty());
        
        buf.push(1000, 100.0, 105.0, 95.0, 102.0, 1000.0);
        assert_eq!(buf.len(), 1);
        
        let bar = buf.get(0).unwrap();
        assert_eq!(bar.0, 1000); // timestamp
        assert_eq!(bar.4, 102.0); // close
    }
    
    #[test]
    fn test_ema_incremental() {
        let mut ema = EmaState::new(10);
        let prices = [100.0, 101.0, 102.0, 101.5, 103.0];
        
        let mut last = 0.0;
        for p in prices {
            last = ema.update(p);
        }
        assert!(last > 100.0 && last < 103.0);
    }
    
    #[test]
    fn test_rsi_incremental() {
        let mut rsi = RsiState::new(14);
        
        // Simulate uptrend
        let mut last_rsi = 50.0;
        for i in 0..20 {
            last_rsi = rsi.update(100.0 + i as f32);
        }
        
        // RSI should be high in uptrend
        assert!(last_rsi > 60.0, "RSI in uptrend should be > 60, got {}", last_rsi);
    }
    
    #[test]
    fn test_fast_calculator() {
        let mut calc = FastIndicatorCalculator::new();
        
        // Feed some data with realistic price movement (not pure uptrend)
        for i in 0..100 {
            // Add some noise to avoid pure uptrend (which gives RSI=100)
            let noise = if i % 3 == 0 { -0.05 } else { 0.1 };
            let price = 100.0 + (i as f32 * noise);
            calc.update(price, price + 1.0, price - 1.0, price, 1000.0);
        }
        
        // Check we have values
        let rsi = calc.get(0);
        assert!(rsi >= 0.0 && rsi <= 100.0, "RSI should be 0-100, got {}", rsi);
        
        let ema9 = calc.get(4);
        assert!(ema9 > 0.0);
    }
    
    #[test]
    fn test_shared_cache() {
        let cache = SharedIndicatorCache::new(100);
        
        let values = [50.0f32; 25];
        cache.update(&values);
        
        assert_eq!(cache.get_latest(0), 50.0);
    }
}
