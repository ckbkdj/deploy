# syntax=docker/dockerfile:1.7
FROM golang:1.27.0-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /out/risk-gateway ./cmd/gateway && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/mock-upstream ./cmd/mock-upstream

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata curl && addgroup -S -g 10001 risk && adduser -S -D -H -u 10001 -G risk risk
COPY --from=build /out/risk-gateway /out/mock-upstream /usr/local/bin/
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/risk-gateway"]
