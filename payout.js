#!/usr/bin/env bun

import { DedotClient, WsProvider } from 'dedot';
import { Worker } from 'worker_threads';
import { cpus } from 'os';
import { program } from 'commander';
import { promises as fs } from 'fs';
import { PayoutCache } from './cache.js';
import { Keyring } from '@polkadot/keyring';
import { cryptoWaitReady } from '@polkadot/util-crypto';

// Constants
const ENDPOINTS = {
  polkadot: 'wss://rpc.polkadot.io',
  kusama: 'wss://kusama-rpc.polkadot.io',
  westend: 'wss://westend-rpc.polkadot.io',
};

const DEFAULT_WORKERS = Math.min(cpus().length, 4);
const BATCH_SIZE = 10; // Number of payouts per batch transaction
const BATCH_DELAY_MS = 2000; // Delay between batch submissions
const NOMINATORS_PER_PAGE = 512; // Substrate constant for paged rewards

// Helper functions
const loadStashes = async (filePath) => {
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    const data = JSON.parse(content);
    
    // Handle different formats
    if (Array.isArray(data)) {
      // Simple array format
      return data;
    } else if (typeof data === 'object') {
      if (data.stashes && Array.isArray(data.stashes)) {
        // Object with stashes property
        return data.stashes;
      } else if (data.kusama || data.polkadot) {
        // Multi-chain format - need to specify which chain
        console.log('Multi-chain stashes file detected. Use --all to check all chains.');
        return null;
      }
    } else if (typeof data === 'string') {
      return [data];
    }
    
    throw new Error('Invalid stashes format');
  } catch (error) {
    console.error(`Error loading stashes from ${filePath}:`, error.message);
    process.exit(1);
  }
};

const loadMultiChainStashes = async (filePath) => {
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    const data = JSON.parse(content);
    
    if (data.kusama || data.polkadot) {
      return data;
    }
    
    // Convert simple format to multi-chain
    return {
      kusama: { stashes: Array.isArray(data) ? data : data.stashes || [] }
    };
  } catch (error) {
    console.error(`Error loading stashes from ${filePath}:`, error.message);
    process.exit(1);
  }
};

const loadKeyFromFile = async (keyPath) => {
  try {
    const content = await fs.readFile(keyPath, 'utf-8');
    // Remove any whitespace/newlines
    const key = content.trim();
    
    if (!key) {
      throw new Error('Key file is empty');
    }
    
    // Basic validation - check if it looks like a valid key
    // Could be mnemonic (words), hex seed, or derivation path
    if (key.includes('//') || key.split(' ').length >= 12 || key.startsWith('0x')) {
      return key;
    }
    
    throw new Error('Invalid key format in file');
  } catch (error) {
    if (error.code === 'ENOENT') {
      console.error(`Key file not found: ${keyPath}`);
    } else {
      console.error(`Error reading key file: ${error.message}`);
    }
    process.exit(1);
  }
};

const scanWithWorkers = async (stashes, chain, currentEra, historyDepth, workers, cache) => {
  const chunks = Array.from({ length: workers }, (_, i) => 
    stashes.filter((_, idx) => idx % workers === i)
  );
  
  const endpoint = chain in ENDPOINTS ? ENDPOINTS[chain] : chain;
  const startEra = Math.max(0, currentEra - historyDepth);
  const endEra = currentEra - 1; // Current era is not claimable
  
  console.log(`Scanning eras ${startEra} to ${endEra} for ${stashes.length} stashes...`);
  console.log(`Using ${workers} workers`);
  
  const workerPromises = chunks.map((chunk, i) => 
    new Promise((resolve, reject) => {
      const worker = new Worker('./worker.js');
      
      worker.on('message', (result) => {
        if (result.error) {
          reject(new Error(result.error));
        } else {
          resolve(result);
        }
      });
      
      worker.on('error', reject);
      
      worker.postMessage({
        stashes: chunk,
        endpoint,
        startEra,
        endEra,
        chain,
        useCache: !!cache,
        redisUrl: cache?.redis?.options?.url
      });
    })
  );
  
  const results = await Promise.all(workerPromises);
  return results.flat();
};

// Core functions
const createClient = async (chain) => {
  const endpoint = chain in ENDPOINTS ? ENDPOINTS[chain] : chain;
  const provider = new WsProvider(endpoint);
  const client = await DedotClient.new(provider);
  
  // Make sure client is connected
  if (!client) {
    throw new Error('Failed to create client');
  }
  
  return client;
};

const getCurrentEra = async (client) => {
  const activeEra = await client.query.staking.activeEra();
  
  // Dedot returns the Option<ActiveEraInfo> directly as the object when Some
  if (activeEra && typeof activeEra.index !== 'undefined') {
    return Number(activeEra.index);
  }
  
  throw new Error('No active era found');
};

const getHistoryDepth = (client) => {
  const historyDepth = client.consts.staking.historyDepth;
  return Number(historyDepth);
};

const submitPayouts = async (client, unclaimed, signer) => {
  const total = unclaimed.length;
  if (!total) return;
  
  console.log(`\nFound ${total} unclaimed payouts`);
  
  // Pre-flight checks
  const { partialFee } = await client.tx.staking.payoutStakers(
    unclaimed[0].validator, 
    unclaimed[0].era, 
    unclaimed[0].pages[0]
  ).paymentInfo(signer.address);
  
  const totalFee = partialFee * BigInt(total);
  const { nonce, data: { free } } = await client.query.system.account(signer.address);
  
  if (free < totalFee * 2n) {
    throw new Error(`Insufficient balance: ${free} < ${totalFee * 2n}`);
  }
  
  console.log(`Fee: ${partialFee} per tx, ${totalFee} total`);
  console.log(`Nonce: ${nonce}\n`);
  
  // Build transactions
  const txs = [];
  let n = nonce;
  
  for (const { validator, era, pages } of unclaimed) {
    for (const page of pages) {
      txs.push({
        tx: client.tx.staking.payoutStakers(validator, era, page),
        nonce: n++,
        validator: validator.substring(0, 8),
        era,
        page
      });
    }
  }
  
  // Submit all
  const results = await Promise.allSettled(
    txs.map(({ tx, nonce, validator, era, page }) =>
      tx.signAndSend(signer, { nonce })
        .untilFinalized()
        .then(r => ({ ok: !r.dispatchError, validator, era, page }))
        .catch(e => ({ ok: false, validator, era, page, err: e.message }))
    )
  );
  
  // Report
  const ok = results.filter(r => r.value?.ok).length;
  const failed = results.filter(r => !r.value?.ok);
  
  console.log(`\n✅ ${ok}/${total}`);
  
  if (failed.length) {
    console.log('\nFailed:');
    failed.forEach(({ value: v }) => 
      console.log(`  ${v.validator} era ${v.era}: ${v.err || 'dispatch error'}`)
    );
  }
};

// API functions (exported for library use)
export const scanPayouts = async (
  stashes,
  chain,
  workers = DEFAULT_WORKERS,
  useCache = false,
  customStartEra = null,
  customEndEra = null
) => {
  console.log(`Connecting to ${chain} chain...`);
  const client = await createClient(chain);
  let cache;
  
  if (useCache) {
    cache = new PayoutCache();
    await cache.connect();
  }
  
  try {
    console.log('Connected and ready');
    
    const currentEra = await getCurrentEra(client);
    console.log(`Current era: ${currentEra}`);
    
    const historyDepth = getHistoryDepth(client);
    console.log(`History depth: ${historyDepth}`);
    
    // Use custom era range if provided
    const startEra = customStartEra !== null ? customStartEra : Math.max(0, currentEra - historyDepth);
    const endEra = customEndEra !== null ? customEndEra : currentEra - 1;
    
    return await scanWithWorkersCustomRange(stashes, chain, startEra, endEra, workers, cache);
  } catch (error) {
    console.error('Error during scan:', error);
    throw error;
  } finally {
    await client.disconnect();
    if (cache) await cache.disconnect();
  }
};

const scanWithWorkersCustomRange = async (stashes, chain, startEra, endEra, workers, cache) => {
  const chunks = Array.from({ length: workers }, (_, i) => 
    stashes.filter((_, idx) => idx % workers === i)
  );
  
  const endpoint = chain in ENDPOINTS ? ENDPOINTS[chain] : chain;
  
  console.log(`Scanning eras ${startEra} to ${endEra} for ${stashes.length} stashes...`);
  console.log(`Using ${workers} workers`);
  
  const workerPromises = chunks.map((chunk, i) => 
    new Promise((resolve, reject) => {
      const worker = new Worker('./worker.js');
      
      worker.on('message', (result) => {
        if (result.error) {
          reject(new Error(result.error));
        } else {
          resolve(result);
        }
      });
      
      worker.on('error', reject);
      
      worker.postMessage({
        stashes: chunk,
        endpoint,
        startEra,
        endEra,
        chain,
        useCache: !!cache,
        redisUrl: cache?.redis?.options?.url
      });
    })
  );
  
  const results = await Promise.all(workerPromises);
  return results.flat();
};

export const executePayout = async (stashes, chain, keyPath, dryRun = false) => {
  const client = await createClient(chain);
  
  try {
    const unclaimed = await scanPayouts(stashes, chain);
    
    if (unclaimed.length === 0) {
      console.log('No unclaimed payouts found');
      return;
    }
    
    if (dryRun) {
      console.log('\n🔍 DRY RUN - Not submitting transactions');
      console.log('\nPayouts that would be submitted:');
      unclaimed.forEach(({ validator, era, pages }) => {
        console.log(`  ${validator} - Era ${era} - Pages: ${pages.join(', ')}`);
      });
      return;
    }
    
    // Load key from file
    const keyUri = await loadKeyFromFile(keyPath);
    
    // Initialize keyring
    await cryptoWaitReady();
    const keyring = new Keyring({ type: 'sr25519' });
    
    // Add account from URI (can be mnemonic, seed, or derived path)
    const signer = keyring.addFromUri(keyUri);
    console.log(`\nUsing account: ${signer.address}`);
    
    // Check account balance
    const accountInfo = await client.query.system.account(signer.address);
    const free = accountInfo.data.free;
    console.log(`Account balance: ${free.toString()}`);
    
    if (free === 0n) {
      throw new Error('Account has zero balance - cannot pay transaction fees');
    }
    
    await submitPayouts(client, unclaimed, signer);
  } finally {
    await client.disconnect();
  }
};

// CLI
program
  .name('payout')
  .description('Substrate validator payout utility')
  .version('1.0.0');

program
  .command('check')
  .description('Check for unclaimed payouts')
  .requiredOption('-s, --stashes <file>', 'JSON file containing stash addresses')
  .option('-c, --chain <chain>', 'Chain to connect to', 'kusama')
  .option('-w, --workers <number>', 'Number of worker threads', parseInt, DEFAULT_WORKERS)
  .option('--cache', 'Use Redis cache', false)
  .option('--all', 'Check all chains in multi-chain stashes file', false)
  .option('--era-range <start-end>', 'Check specific era range (e.g., 8136-8148)')
  .action(async (options) => {
    try {
      // Parse era range if provided
      let startEra, endEra;
      if (options.eraRange) {
        const [start, end] = options.eraRange.split('-').map(Number);
        if (isNaN(start) || isNaN(end)) {
          console.error('Invalid era range format. Use: --era-range START-END');
          process.exit(1);
        }
        startEra = start;
        endEra = end;
        console.log(`Checking specific era range: ${startEra} to ${endEra}`);
      }
      
      if (options.all) {
        // Multi-chain check
        const multiChainData = await loadMultiChainStashes(options.stashes);
        let totalUnclaimed = 0;
        
        for (const [chain, config] of Object.entries(multiChainData)) {
          console.log(`\n🔍 Checking ${chain.toUpperCase()} chain...`);
          
          const endpoint = config.endpoint || ENDPOINTS[chain];
          if (!endpoint) {
            console.log(`⚠️  No endpoint configured for ${chain}`);
            continue;
          }
          
          const unclaimed = await scanPayouts(
            config.stashes,
            endpoint,
            options.workers,
            options.cache,
            startEra,
            endEra
          );
          
          totalUnclaimed += unclaimed.length;
          
          if (unclaimed.length === 0) {
            console.log(`✅ All payouts claimed on ${chain}!`);
          } else {
            console.log(`⚠️  Found ${unclaimed.length} unclaimed payouts on ${chain}:`);
            unclaimed.forEach(({ validator, era, pages }) => {
              console.log(`  ${validator} - Era ${era} - Pages: ${pages.join(', ')}`);
            });
          }
        }
        
        console.log(`\n📊 Total unclaimed payouts across all chains: ${totalUnclaimed}`);
      } else {
        // Single chain check
        const stashesData = await loadStashes(options.stashes);
        
        if (!stashesData) {
          // Multi-chain file but no --all flag
          const multiChainData = await loadMultiChainStashes(options.stashes);
          const chainData = multiChainData[options.chain];
          
          if (!chainData) {
            console.error(`Chain '${options.chain}' not found in stashes file`);
            console.log('Available chains:', Object.keys(multiChainData).join(', '));
            process.exit(1);
          }
          
          const endpoint = chainData.endpoint || ENDPOINTS[options.chain] || options.chain;
          const unclaimed = await scanPayouts(
            chainData.stashes,
            endpoint,
            options.workers,
            options.cache,
            startEra,
            endEra
          );
          
          if (unclaimed.length === 0) {
            console.log('\n✅ All payouts claimed!');
          } else {
            console.log(`\n⚠️  Found ${unclaimed.length} unclaimed payouts:`);
            unclaimed.forEach(({ validator, era, pages }) => {
              console.log(`  ${validator} - Era ${era} - Pages: ${pages.join(', ')}`);
            });
          }
        } else {
          // Simple format
          const unclaimed = await scanPayouts(
            stashesData,
            options.chain,
            options.workers,
            options.cache,
            startEra,
            endEra
          );
          
          if (unclaimed.length === 0) {
            console.log('\n✅ All payouts claimed!');
          } else {
            console.log(`\n⚠️  Found ${unclaimed.length} unclaimed payouts:`);
            unclaimed.forEach(({ validator, era, pages }) => {
              console.log(`  ${validator} - Era ${era} - Pages: ${pages.join(', ')}`);
            });
          }
        }
      }
    } catch (error) {
      console.error('Error:', error.message);
      process.exit(1);
    }
  });

program
  .command('submit')
  .description('Submit payout transactions')
  .requiredOption('-s, --stashes <file>', 'JSON file containing stash addresses')
  .requiredOption('-k, --keyfile <file>', 'File containing key (mnemonic/seed/derivation path)')
  .option('-c, --chain <chain>', 'Chain to connect to', 'kusama')
  .option('--dry-run', 'Show what would be submitted without actually submitting', false)
  .option('--era-range <start-end>', 'Submit payouts for specific era range (e.g., 8136-8148)')
  .action(async (options) => {
    try {
      // Handle multi-chain stashes file
      const multiChainData = await loadMultiChainStashes(options.stashes);
      const chainData = multiChainData[options.chain];
      
      if (!chainData) {
        // Try simple format
        const stashes = await loadStashes(options.stashes);
        if (stashes) {
          await executePayout(stashes, options.chain, options.keyfile, options.dryRun);
        } else {
          console.error(`Chain '${options.chain}' not found in stashes file`);
          console.log('Available chains:', Object.keys(multiChainData).join(', '));
          process.exit(1);
        }
      } else {
        const endpoint = chainData.endpoint || ENDPOINTS[options.chain] || options.chain;
        
        // If era range specified, scan only that range
        if (options.eraRange) {
          const [startEra, endEra] = options.eraRange.split('-').map(Number);
          const unclaimed = await scanPayouts(
            chainData.stashes,
            endpoint,
            DEFAULT_WORKERS,
            false,
            startEra,
            endEra
          );
          
          if (unclaimed.length === 0) {
            console.log('No unclaimed payouts found in specified era range');
            return;
          }
          
          if (options.dryRun) {
            console.log('\n🔍 DRY RUN - Not submitting transactions');
            console.log('\nPayouts that would be submitted:');
            unclaimed.forEach(({ validator, era, pages }) => {
              console.log(`  ${validator} - Era ${era} - Pages: ${pages.join(', ')}`);
            });
            return;
          }
          
          // Load key from file
          const keyUri = await loadKeyFromFile(options.keyfile);
          
          // Submit with custom unclaimed list
          const client = await createClient(endpoint);
          try {
            await cryptoWaitReady();
            const keyring = new Keyring({ type: 'sr25519' });
            const signer = keyring.addFromUri(keyUri);
            console.log(`\nUsing account: ${signer.address}`);
            
            await submitPayouts(client, unclaimed, signer);
          } finally {
            await client.disconnect();
          }
        } else {
          // Normal full scan and submit
          await executePayout(chainData.stashes, endpoint, options.keyfile, options.dryRun);
        }
      }
    } catch (error) {
      console.error('Error:', error.message);
      process.exit(1);
    }
  });

program.parse();
