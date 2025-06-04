const { parentPort } = require('worker_threads');
const { DedotClient, WsProvider } = require('dedot');
const { PayoutCache } = require('./cache');

const NOMINATORS_PER_PAGE = 512;

parentPort.on('message', async ({ stashes, endpoint, startEra, endEra, chain, useCache, redisUrl }) => {
  const provider = new WsProvider(endpoint);
  const client = await DedotClient.new(provider);
  const results = [];
  
  let cache;
  if (useCache) {
    cache = new PayoutCache({ url: redisUrl });
    await cache.connect();
  }
  
  try {
    for (const stash of stashes) {
      const eraChecks = [];
      
      for (let era = startEra; era <= endEra; era++) {
        eraChecks.push((async () => {
          try {
            // Check cache first
            if (cache) {
              const cached = await cache.getEra(chain, stash, era);
              if (cached) {
                const unclaimed = [];
                for (let p = 0; p < cached.totalPages; p++) {
                  if (!cached.claimedPages.includes(p)) unclaimed.push(p);
                }
                return unclaimed.length ? { validator: stash, era, pages: unclaimed } : null;
              }
            }
            
            // Check if erasStakersOverview exists (newer chains)
            let exposure, pageCount;
            
            if (client.query.staking.erasStakersOverview) {
              // New paged exposure system
              const overview = await client.query.staking.erasStakersOverview([era, stash]);
              
              if (!overview || !overview.total || overview.total === 0n) {
                // Validator was not active in this era
                if (cache) {
                  await cache.setEra(chain, {
                    validator: stash,
                    era,
                    claimedPages: [],
                    totalPages: 0,
                    lastChecked: Date.now()
                  });
                }
                return null;
              }
              
              pageCount = Number(overview.pageCount || 0);
              exposure = overview;
            } else {
              // Fallback to old erasStakersClipped
              exposure = await client.query.staking.erasStakersClipped([era, stash]);
              
              if (!exposure || !exposure.total || exposure.total === 0n) {
                // Validator was not active in this era
                if (cache) {
                  await cache.setEra(chain, {
                    validator: stash,
                    era,
                    claimedPages: [],
                    totalPages: 0,
                    lastChecked: Date.now()
                  });
                }
                return null;
              }
              
              // Calculate pages for old system
              pageCount = Math.ceil((exposure.others?.length || 0) / NOMINATORS_PER_PAGE);
            }
            
            // Get claimed rewards
            const claimed = await client.query.staking.claimedRewards([era, stash]);
            
            // Debug logging for specific eras
            if (era >= 136 && era <= 148) {
              console.log(`\nDebug Era ${era} for ${stash.substring(0, 8)}...`);
              console.log('  Exposure total:', exposure.total?.toString());
              console.log('  Page count:', pageCount);
              console.log('  Claimed rewards raw:', claimed);
              console.log('  Claimed type:', typeof claimed);
              console.log('  Is array:', Array.isArray(claimed));
            }
            
            // Handle claimed rewards - it might be:
            // 1. Empty array [] when nothing is claimed
            // 2. Array of page numbers [0, 1, 2] when pages are claimed
            // 3. null/undefined if the era is too old
            let claimedPages = [];
            
            if (claimed === null || claimed === undefined) {
              // No claims data - assume nothing claimed
              claimedPages = [];
            } else if (Array.isArray(claimed)) {
              claimedPages = claimed.map(p => Number(p));
            } else if (claimed && typeof claimed === 'object' && claimed.length !== undefined) {
              // Handle Vec-like objects
              claimedPages = Array.from(claimed).map(p => Number(p));
            }
            
            // Special case: if there are no pages (no nominators), 
            // the validator still needs to claim era rewards
            if (pageCount === 0 && claimedPages.length === 0) {
              // Check if validator has any exposure at all
              if (exposure.total && exposure.total > 0n) {
                // Has stake but no nominators - still needs to claim
                pageCount = 1; // Treat as single page
              }
            }
            
            // Find unclaimed pages
            const unclaimed = [];
            for (let p = 0; p < pageCount; p++) {
              if (!claimedPages.includes(p)) {
                unclaimed.push(p);
              }
            }
            
            // Debug for specific eras
            if (era >= 136 && era <= 148) {
              console.log(`  Claimed pages:`, claimedPages);
              console.log(`  Unclaimed pages:`, unclaimed);
              console.log(`  Has unclaimed:`, unclaimed.length > 0);
            }
            
            // Cache result
            if (cache) {
              await cache.setEra(chain, {
                validator: stash,
                era,
                claimedPages,
                totalPages: pageCount,
                lastChecked: Date.now()
              });
            }
            
            return unclaimed.length ? { validator: stash, era, pages: unclaimed } : null;
          } catch (err) {
            console.error(`Error checking era ${era} for ${stash}:`, err.message);
            return null;
          }
        })());
      }
      
      const eraResults = await Promise.all(eraChecks);
      results.push(...eraResults.filter(Boolean));
    }
    
    parentPort.postMessage(results);
  } catch (error) {
    console.error('Worker error:', error);
    parentPort.postMessage({ error: error.message });
  } finally {
    await client.disconnect();
    if (cache) await cache.disconnect();
  }
});
