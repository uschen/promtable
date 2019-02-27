#!/usr/bin/env bash
#
# Generate all protobuf bindings.
# Run from repository root.
set -e
set -u

if ! [[ "$0" =~ "scripts/genproto.sh" ]]; then
	echo "must be run from repository root"
	exit 255
fi

if ! [[ $(protoc --version) =~ "3.6.0" ]]; then
	echo "could not find protoc 3.6.0, is it installed + in PATH?"
	exit 255
fi

# echo "installing plugins"
# GO111MODULE=on go mod download

# INSTALL_PKGS="golang.org/x/tools/cmd/goimports github.com/gogo/protobuf/protoc-gen-gogofast"
# for pkg in ${INSTALL_PKGS}; do
#     GO111MODULE=on go install -mod=vendor "$pkg"
# done

PROM_ROOT="${PWD}"
PROM_PATH="${PROM_ROOT}/prompb"
GOGOPROTO_ROOT="${GOPATH}/src/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

DIRS="prompb"

echo "generating code"
for dir in ${DIRS}; do
	pushd ${dir}
		protoc --gogofast_out=plugins=grpc:. -I=. \
            -I="${GOGOPROTO_PATH}" \
            -I="${PROM_PATH}" \
            ./*.proto

		sed -i.bak -E 's/import _ \"github.com\/gogo\/protobuf\/gogoproto\"//g' *.pb.go
		sed -i.bak -E 's/import _ \"google\/protobuf\"//g' *.pb.go
		sed -i.bak -E 's/golang\/protobuf/gogo\/protobuf/g' *.go
		rm -f *.bak
		goimports -w ./*.pb.go
	popd
done
