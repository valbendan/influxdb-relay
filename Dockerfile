FROM opensuse:42.2

MAINTAINER valbendan

RUN mkdir /etc/influxdb-relay
COPY influxdb-relay /usr/bin
COPY dev.toml       /etc/influxdb-relay/dev.toml
RUN chmod 555 /usr/bin/influxdb-relay

ENTRYPOINT ["/usr/bin/influxdb-relay", "-config", "/etc/influxdb-relay/dev.toml"]
