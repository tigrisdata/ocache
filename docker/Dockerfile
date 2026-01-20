FROM alpine:3.19

ARG TARGETARCH

# Install minimal runtime dependencies
RUN apk add --no-cache ca-certificates tzdata wget

# Create non-root user
RUN addgroup -g 1000 ocache && \
    adduser -u 1000 -G ocache -h /home/ocache -D ocache

# Copy pre-built binaries (downloaded by workflow)
COPY --chmod=755 bin/${TARGETARCH}/ocache /usr/local/bin/ocache
COPY --chmod=755 bin/${TARGETARCH}/ocachecli /usr/local/bin/ocachecli

# Create data directory
RUN mkdir -p /data && chown -R ocache:ocache /data

USER ocache
VOLUME ["/data"]
EXPOSE 9000 9001 7000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget -q --spider http://localhost:9001/health || exit 1

ENTRYPOINT ["ocache"]
CMD ["-disk", "/data"]
