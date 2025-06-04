FROM oven/bun:1-alpine

RUN apk add --no-cache tini

WORKDIR /app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

COPY . .

EXPOSE 3000

ENTRYPOINT ["tini", "--"]
CMD ["bun", "run", "payout.js"]
