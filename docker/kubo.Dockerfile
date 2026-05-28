FROM ipfs/kubo:latest AS kubo

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates iproute2 curl jq && rm -rf /var/lib/apt/lists/*
COPY --from=kubo /usr/local/bin/ipfs /usr/local/bin/ipfs
ENV PATH="/usr/local/bin:${PATH}"
