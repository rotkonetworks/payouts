#!/usr/bin/env bun

import { DedotClient, WsProvider } from 'dedot';
import { createClient } from 'redis';

// Staking moved to Asset Hub via AHM — point at AH, not relay.
const ENDPOINTS = {
  polkadot: process.env.RPC_POLKADOT || 'wss://asset-hub-polkadot.rotko.net',
  kusama: process.env.RPC_KUSAMA || 'wss://asset-hub-kusama.rotko.net',
  paseo: process.env.RPC_PASEO || 'wss://asset-hub-paseo.rotko.net',
};

async function checkHealth() {
  // Check Redis
  const redis = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
  try {
    await redis.connect();
    await redis.ping();
    await redis.disconnect();
  } catch (e) {
    console.error('Redis unhealthy:', e.message);
    process.exit(1);
  }

  // Check RPC endpoints
  for (const [chain, endpoint] of Object.entries(ENDPOINTS)) {
    try {
      const provider = new WsProvider(endpoint);
      const client = await DedotClient.new(provider);
      await client.query.system.chain();
      await client.disconnect();
    } catch (e) {
      console.error(`${chain} RPC unhealthy:`, e.message);
      process.exit(1);
    }
  }

  console.log('Health check passed');
  process.exit(0);
}

checkHealth().catch(e => {
  console.error('Health check failed:', e);
  process.exit(1);
});
