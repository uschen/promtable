FROM alpine
LABEL maintainer="Chen Liang <1@chen.dev>"

COPY promtable /bin/promtable

RUN mkdir -p /promtable

USER nobody
EXPOSE 9202
VOLUME ["/promtable"]
WORKDIR /promtable
ENTRYPOINT [ "/bin/promtable" ]
CMD []
