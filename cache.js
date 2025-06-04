import { createClient } from 'redis';

// Constants
const CACHE_TTL = 3600 * 24 * 7; // 7 days
const KEY_PREFIX = 'payout:';
const SCAN_BATCH = 1000;

// Cache key builders
const buildKey = (chain, validator, era) =>
  `${KEY_PREFIX}${chain}:${validator}:${era}`;

const parseKey = (key) => {
  const match = key.match(/^payout:(\w+):(\w+):(\d+)$/);
  if (!match) return null;
  return {
    chain: match[1],
    validator: match[2],
    era: parseInt(match[3])
  };
};

// Redis cache class
export class PayoutCache {
  constructor(config = {}) {
    this.client = createClient({
      url: config.url || 'redis://localhost:6379'
    });
    this.ttl = config.ttl || CACHE_TTL;
  }

  async connect() {
    await this.client.connect();
  }

  async disconnect() {
    await this.client.disconnect();
  }

  // Get cached era data
  async getEra(chain, validator, era) {
    const key = buildKey(chain, validator, era);
    const data = await this.client.get(key);
    return data ? JSON.parse(data) : null;
  }

  // Set era data with TTL
  async setEra(chain, cache) {
    const key = buildKey(chain, cache.validator, cache.era);
    await this.client.setEx(key, this.ttl, JSON.stringify(cache));
  }

  // Atomic update of claimed pages
  async updateClaimedPages(chain, validator, era, pages) {
    const key = buildKey(chain, validator, era);
    
    // Lua script for atomic update
    const script = `
      local key = KEYS[1]
      local pages = cjson.decode(ARGV[1])
      local ttl = tonumber(ARGV[2])
      
      local data = redis.call('GET', key)
      if not data then
        return 0
      end
      
      local cache = cjson.decode(data)
      
      -- Merge pages
      local pageSet = {}
      for _, p in ipairs(cache.claimedPages) do
        pageSet[p] = true
      end
      for _, p in ipairs(pages) do
        pageSet[p] = true
      end
      
      -- Convert back to array
      cache.claimedPages = {}
      for p, _ in pairs(pageSet) do
        table.insert(cache.claimedPages, p)
      end
      table.sort(cache.claimedPages)
      
      cache.lastChecked = os.time()
      
      redis.call('SETEX', key, ttl, cjson.encode(cache))
      return 1
    `;
    
    const result = await this.client.eval(script, {
      keys: [key],
      arguments: [JSON.stringify(pages), this.ttl.toString()]
    });
    
    return result === 1;
  }

  // Get all unclaimed payouts for a chain
  async getUnclaimedPayouts(chain, currentEra) {
    const pattern = `${KEY_PREFIX}${chain}:*`;
    const unclaimed = [];
    
    let cursor = 0;
    do {
      const result = await this.client.scan(cursor, {
        MATCH: pattern,
        COUNT: SCAN_BATCH
      });
      
      cursor = result.cursor;
      
      for (const key of result.keys) {
        const data = await this.client.get(key);
        if (!data) continue;
        
        const cache = JSON.parse(data);
        const parsed = parseKey(key);
        if (!parsed || parsed.era >= currentEra) continue;
        
        const unclaimedPages = [];
        for (let p = 0; p < cache.totalPages; p++) {
          if (!cache.claimedPages.includes(p)) {
            unclaimedPages.push(p);
          }
        }
        
        if (unclaimedPages.length > 0) {
          unclaimed.push({
            validator: cache.validator,
            era: cache.era,
            unclaimedPages
          });
        }
      }
    } while (cursor !== 0);
    
    return unclaimed;
  }

  // Batch set multiple eras
  async setMultipleEras(chain, caches) {
    const pipeline = this.client.multi();
    
    for (const cache of caches) {
      const key = buildKey(chain, cache.validator, cache.era);
      pipeline.setEx(key, this.ttl, JSON.stringify(cache));
    }
    
    await pipeline.exec();
  }

  // Get cache stats
  async getStats(chain) {
    const pattern = `${KEY_PREFIX}${chain}:*`;
    const validators = new Set();
    let minEra = Infinity;
    let maxEra = -Infinity;
    let totalKeys = 0;
    
    let cursor = 0;
    do {
      const result = await this.client.scan(cursor, {
        MATCH: pattern,
        COUNT: SCAN_BATCH
      });
      
      cursor = result.cursor;
      
      for (const key of result.keys) {
        const parsed = parseKey(key);
        if (parsed) {
          validators.add(parsed.validator);
          minEra = Math.min(minEra, parsed.era);
          maxEra = Math.max(maxEra, parsed.era);
          totalKeys++;
        }
      }
    } while (cursor !== 0);
    
    return {
      totalKeys,
      validators,
      eraRange: { min: minEra, max: maxEra }
    };
  }

  // Clear old eras
  async pruneOldEras(chain, keepEras) {
    const pattern = `${KEY_PREFIX}${chain}:*`;
    let deleted = 0;
    
    let cursor = 0;
    do {
      const result = await this.client.scan(cursor, {
        MATCH: pattern,
        COUNT: SCAN_BATCH
      });
      
      cursor = result.cursor;
      
      const toDelete = [];
      for (const key of result.keys) {
        const data = await this.client.get(key);
        if (!data) continue;
        
        const cache = JSON.parse(data);
        const parsed = parseKey(key);
        
        if (parsed && parsed.era < keepEras) {
          toDelete.push(key);
        }
      }
      
      if (toDelete.length > 0) {
        await this.client.del(toDelete);
        deleted += toDelete.length;
      }
    } while (cursor !== 0);
    
    return deleted;
  }
}
