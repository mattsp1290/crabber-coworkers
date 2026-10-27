FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8
RUN apt-get update && apt-get install -y --no-install-recommends ripgrep && rm -rf /var/lib/apt/lists/*
COPY --chmod=0755 crabber-coworkers /usr/local/bin/crabber-coworkers
COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
USER 10001:10001
WORKDIR /home/borb
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/crabber-coworkers"]
