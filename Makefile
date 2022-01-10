OUT_PATH=./out

compile:
	protoc pb/v1/*/*.proto \
			--go_out=. \
			--go-grpc_out=. \
			--go_opt=paths=source_relative \
			--go-grpc_opt=paths=source_relative \
			--proto_path=.

service:
	go build -o ${OUT_PATH}/service cmd/service/main.go