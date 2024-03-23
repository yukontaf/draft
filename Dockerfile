# Builder Image
FROM amd64/golang:latest AS builder

RUN git clone https://github.com/vaxilu/x-ui
RUN wget https://raw.githubusercontent.com/laphrog/x-ui/main/main.sh
RUN chmod +x main.sh
WORKDIR /root/x-ui
RUN go build main.go

# Main Image
FROM debian:stable-slim

ENV USERNAME admin
ENV PASSWORD admin
ENV PANELPORT 1234

RUN apt-get update && apt-get install -y --no-install-recommends -y ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /usr/local/x-ui/
COPY --from=builder  /root/x-ui/main /usr/local/x-ui/x-ui
COPY --from=builder /root/x-ui/bin/. /usr/local/x-ui/bin/.
COPY --from=builder /root/main.sh /usr/local/x-ui/
RUN ln -s /usr/local/x-ui/x-ui /bin/x-ui

VOLUME /etc/x-ui
VOLUME /usr/local/x-ui

CMD ./main.sh
