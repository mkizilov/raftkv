module github.com/you/raftkv

go 1.24.0

toolchain go1.24.10

tool (
	github.com/bufbuild/buf/cmd/buf
	github.com/golangci/golangci-lint/v2/cmd/golangci-lint
	google.golang.org/grpc/cmd/protoc-gen-go-grpc
	google.golang.org/protobuf/cmd/protoc-gen-go
)

require (
	github.com/anishathalye/porcupine v1.1.0
	github.com/bufbuild/buf v1.50.0
	github.com/golangci/golangci-lint/v2 v2.1.6
	google.golang.org/grpc v1.71.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.5.1
	google.golang.org/protobuf v1.36.6
)
