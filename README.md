# rotko payouts
use bun or node, run docker compose for redis and modify stashes to match yours,
set -c network and -k keyfile(private_key). atm without proxy

```
git clone https://github.com/rotkonetworks/payouts
docker compose up -d
bun install
bun payout.js submit -s stashes.json -c kusama -k .keyfile
```
