// Direct vitest debug
test('campaign debug', async () => {
  const campaign = vi.fn();
  const consensus = {
    start: vi.fn(),
    stop: vi.fn(),
    propose: vi.fn(),
    isLeader: vi.fn(),
    getLeader: vi.fn(),
    getCommitIndex: vi.fn(),
    registerShard: vi.fn(),
    removeShard: vi.fn(),
    notifyTopologyChange: vi.fn(),
    getHealthStatus: vi.fn(),
    campaign: campaign,
    on: vi.fn(),
    off: vi.fn(),
    setForceLeader: vi.fn(),
  };
  
  // Direct call
  await consensus.campaign('0');
  console.log('After direct call, campaign calls:', campaign.mock.calls.length);
  expect(campaign.mock.calls.length).toBe(1);
});
