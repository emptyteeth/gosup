BINARY  := gosup
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w
# Target architecture: native by default; cross-compile with `make build ARCH=arm64`.
ARCH ?=

.PHONY: build build-all test race vet fmt clean

build:
	CGO_ENABLED=0 GOARCH=$(ARCH) go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) .

# Build both target architectures at once; artifacts carry a suffix.
build-all:
	@for a in amd64 arm64; do \
		echo "building $$a"; \
		CGO_ENABLED=0 GOARCH=$$a go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY)-$$a . || exit 1; \
	done

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

clean:
	rm -f $(BINARY)
