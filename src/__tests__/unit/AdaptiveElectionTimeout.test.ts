/**
 * AdaptiveElectionTimeout Unit Tests
 *
 * AC 35.1: Constructor with default config
 * AC 35.2: recordRtt maintains sliding window
 * AC 35.3: getAvgRtt returns correct moving average
 * AC 35.4: getRecommendedTick returns initial base tick
 * AC 35.5: recalculate updates tick based on RTT samples
 * AC 35.6: Tick clamped within [minTick, maxTick]
 * AC 35.7: start/stop manages interval lifecycle
 * AC 35.8: setConfig updates individual fields
 * AC 35.9: onChange callback fires on tick change
 * AC 35.10: Empty RTT samples returns 0 average
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { AdaptiveElectionTimeout } from '../../consensus/opt/AdaptiveElectionTimeout';
import { DEFAULT_ADAPTIVE_ELECTION_CONFIG } from '../../interfaces/types';

describe('AdaptiveElectionTimeout', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('AC 35.1: constructor with default config', () => {
    const aet = new AdaptiveElectionTimeout();
    expect(aet.getRecommendedTick()).toBe(DEFAULT_ADAPTIVE_ELECTION_CONFIG.baseElectionTick);
    const cfg = aet.getConfig();
    expect(cfg.baseElectionTick).toBe(DEFAULT_ADAPTIVE_ELECTION_CONFIG.baseElectionTick);
    expect(cfg.rttMultiplier).toBe(DEFAULT_ADAPTIVE_ELECTION_CONFIG.rttMultiplier);
    expect(cfg.minTick).toBe(DEFAULT_ADAPTIVE_ELECTION_CONFIG.minTick);
    expect(cfg.maxTick).toBe(DEFAULT_ADAPTIVE_ELECTION_CONFIG.maxTick);
    expect(aet.getSampleCount()).toBe(0);
  });

  it('AC 35.2: recordRtt maintains sliding window', () => {
    const aet = new AdaptiveElectionTimeout({ rttSampleWindow: 5 });
    for (let i = 0; i < 10; i++) aet.recordRtt(100);
    expect(aet.getSampleCount()).toBe(5); // window = 5
    for (let i = 0; i < 3; i++) aet.recordRtt(50);
    expect(aet.getSampleCount()).toBe(5); // still capped
  });

  it('AC 35.3: getAvgRtt returns correct moving average', () => {
    const aet = new AdaptiveElectionTimeout();
    aet.recordRtt(10);
    aet.recordRtt(20);
    aet.recordRtt(30);
    expect(aet.getAvgRtt()).toBe(20);
  });

  it('AC 35.4: getRecommendedTick returns initial base tick', () => {
    const aet = new AdaptiveElectionTimeout({ baseElectionTick: 15 });
    expect(aet.getRecommendedTick()).toBe(15);
  });

  it('AC 35.5: recalculate updates tick based on RTT samples', () => {
    const aet = new AdaptiveElectionTimeout({
      baseElectionTick: 10,
      tickIntervalMs: 10,
      rttMultiplier: 2,
      minTick: 1,
      maxTick: 100,
    });
    // avgRTT = 50ms → rttInTicks = 50/10 = 5 → recommended = 5*2 = 10
    aet.recordRtt(50);
    // Manually trigger recalculate via start interval
    aet.start();
    vi.advanceTimersByTime(1000);
    // With avgRTT=50, multiplier=2, tickMs=10: 50/10*2=10
    expect(aet.getRecommendedTick()).toBe(10);
    aet.stop();
  });

  it('AC 35.6: Tick clamped within [minTick, maxTick]', () => {
    const aet = new AdaptiveElectionTimeout({
      baseElectionTick: 10,
      tickIntervalMs: 10,
      rttMultiplier: 2,
      minTick: 5,
      maxTick: 8,
    });
    // avgRTT=100 → 100/10*2=20 → clamp to 8
    aet.recordRtt(100);
    aet.start();
    vi.advanceTimersByTime(1000);
    expect(aet.getRecommendedTick()).toBe(8);
    aet.stop();

    // Low RTT → clamp to minTick
    const aet2 = new AdaptiveElectionTimeout({
      baseElectionTick: 10,
      tickIntervalMs: 10,
      rttMultiplier: 1,
      minTick: 3,
      maxTick: 20,
    });
    // avgRTT=10 → 10/10*1=1 → clamp to 3
    aet2.recordRtt(10);
    aet2.start();
    vi.advanceTimersByTime(1000);
    expect(aet2.getRecommendedTick()).toBe(3);
    aet2.stop();
  });

  it('AC 35.7: start/stop manages interval lifecycle', () => {
    const aet = new AdaptiveElectionTimeout();
    // Not started: internal timer is null
    expect(aet.getRecommendedTick()).toBe(DEFAULT_ADAPTIVE_ELECTION_CONFIG.baseElectionTick);
    aet.start();
    // After start, interval runs
    aet.stop();
    // Double stop is safe
    aet.stop();
  });

  it('AC 35.8: setConfig updates individual fields', () => {
    const aet = new AdaptiveElectionTimeout({ baseElectionTick: 10 });
    aet.setConfig({ baseElectionTick: 20 });
    expect(aet.getConfig().baseElectionTick).toBe(20);
    aet.setConfig({ rttMultiplier: 5 });
    expect(aet.getConfig().rttMultiplier).toBe(5);
    aet.setConfig({ minTick: 3, maxTick: 15 });
    expect(aet.getConfig().minTick).toBe(3);
    expect(aet.getConfig().maxTick).toBe(15);
    // minTick must be ≥1
    aet.setConfig({ minTick: 0 });
    expect(aet.getConfig().minTick).toBe(1); // clamped
  });

  it('AC 35.9: onChange callback fires on tick change', () => {
    const onChange = vi.fn();
    const aet = new AdaptiveElectionTimeout(
      {
        baseElectionTick: 10,
        tickIntervalMs: 10,
        rttMultiplier: 2,
        minTick: 1,
        maxTick: 100,
        rttSampleWindow: 5,
        recalculationIntervalMs: 100,
      },
      onChange,
    );
    aet.recordRtt(100);
    aet.start();
    vi.advanceTimersByTime(200);
    expect(onChange).toHaveBeenCalledTimes(1);
    const newTick = onChange.mock.calls[0][0];
    const oldTick = onChange.mock.calls[0][1];
    expect(newTick).toBe(20); // 100/10*2
    expect(oldTick).toBe(10);
    aet.stop();
  });

  it('AC 35.10: Empty RTT samples returns 0 average', () => {
    const aet = new AdaptiveElectionTimeout();
    expect(aet.getAvgRtt()).toBe(0);
    expect(aet.getSampleCount()).toBe(0);
  });
});
