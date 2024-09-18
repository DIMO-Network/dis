FROM golang:1.22 AS build

RUN useradd -u 10001 dimo

WORKDIR /build
COPY . ./

RUN make dep
RUN make build

# FROM gcr.io/distroless/static AS final
# TODO (Kevin): repalce with distroless/static later
# reuse golang for debugging purposes 
FROM golang:1.22 AS final 

LABEL maintainer="DIMO <hello@dimo.zone>"

USER nonroot:nonroot

COPY --from=build --chown=nonroot:nonroot /build/bin/DIS /
COPY --from=build --chown=nonroot:nonroot /build/sample-config.yaml /benthos.yaml

ENTRYPOINT ["/DIS"]

CMD ["-c", "/benthos.yaml"]