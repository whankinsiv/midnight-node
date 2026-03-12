fn main() {
	let protoc = protoc_bin_vendored::protoc_bin_path().unwrap();
	unsafe {
		std::env::set_var("PROTOC", protoc);
	}

	tonic_build::configure()
		.compile_protos(&["src/grpc/proto/midnight_indexer.proto"], &["src/grpc/proto"])
		.unwrap();
}
