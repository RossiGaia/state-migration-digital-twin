FROM redhat/ubi10-minimal:latest-source

RUN microdnf install -y \
      buildah \
      criu \
      curl \
      bash \
    && microdnf clean all

COPY criu_dump.sh /usr/local/bin/criu_dump.sh
RUN chmod +x /usr/local/bin/criu_dump.sh

ENTRYPOINT ["bash", "-c", "sleep infinity"]
