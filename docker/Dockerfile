FROM node:22-bookworm AS ui-builder
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --include=dev
RUN test -x node_modules/.bin/vue-tsc && test -x node_modules/.bin/vite
COPY ui/index.html ./
COPY ui/tsconfig.json ui/tsconfig.node.json ui/vite.config.ts ./
COPY ui/src ./src
RUN npm run build

FROM golang:1.24 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/gs-peer ./cmd/gs-peer
RUN CGO_ENABLED=0 go build -o /out/gs-client ./cmd/gs-client
RUN CGO_ENABLED=0 go build -o /out/gs-keygen ./cmd/gs-keygen
RUN CGO_ENABLED=0 go build -o /out/benchmark-ui ./cmd/benchmark-ui
RUN CGO_ENABLED=0 go build -o /out/seed ./cmd/seed
RUN CGO_ENABLED=0 go build -o /out/boxo-bitswap-client ./cmd/boxo-bitswap-client

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg iproute2 ffmpeg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /out/gs-peer /usr/local/bin/gs-peer
COPY --from=builder /out/gs-client /usr/local/bin/gs-client
COPY --from=builder /out/gs-keygen /usr/local/bin/gs-keygen
COPY --from=builder /out/benchmark-ui /usr/local/bin/benchmark-ui
COPY --from=builder /out/seed /usr/local/bin/seed
COPY --from=builder /out/boxo-bitswap-client /usr/local/bin/boxo-bitswap-client
COPY --from=ui-builder /src/ui/dist /app/ui-dist
COPY docker/gs-peer.sh /usr/local/bin/gs-peer.sh
COPY docker/update-peers.sh /usr/local/bin/update-peers.sh
COPY docker/shape_fast.sh /usr/local/bin/shape_fast.sh
COPY docker/shape_slow.sh /usr/local/bin/shape_slow.sh
COPY docker/clear-kubo-repo.sh /usr/local/bin/clear-kubo-repo.sh
RUN chmod +x /usr/local/bin/gs-peer.sh /usr/local/bin/update-peers.sh /usr/local/bin/shape_fast.sh /usr/local/bin/shape_slow.sh /usr/local/bin/clear-kubo-repo.sh
ENV PATH="/usr/local/bin:${PATH}"
