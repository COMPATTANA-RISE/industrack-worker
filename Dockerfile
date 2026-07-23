# ============================================================
# IndusTrack — worker (Go 1.25, MQTT → InfluxDB ingester)
# Multi-stage production build: builder → runner
# Build with: docker build --platform linux/amd64 -f worker/Dockerfile worker/
# ============================================================
FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Fully static binary, no cgo, stripped + reproducible paths.
#   -trimpath        strip local build paths from the binary
#   -ldflags="-s -w" drop symbol table + DWARF (~25-30% smaller)
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /out/worker ./cmd/worker

# ------------------------------------------------------------
# Runtime stage
# ------------------------------------------------------------
FROM alpine:3.19 AS runner

# ca-certificates: HTTPS to InfluxDB / MQTT over TLS
# tzdata: time.Time formatting for non-UTC zones
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Non-root UID 1001 / GID 1001 (matches fix-volumes-files-permissions.sh, which
# defaults to chown 1001:1001). `-G` is required — busybox adduser otherwise
# puts the user in `nogroup` (65533).
# User is created before the COPY so --chown avoids a duplicate binary layer.
RUN addgroup --system --gid 1001 golang && \
    adduser --system --uid 1001 -G golang worker

COPY --from=builder --chown=worker:golang /out/worker /app/worker

USER worker

# No HTTP listener — pure MQTT consumer, so no EXPOSE.
ENTRYPOINT ["/app/worker"]
