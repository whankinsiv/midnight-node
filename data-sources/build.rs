fn main() {
	unsafe {
		std::env::set_var(
			"PROTOC",
			protoc_bin_vendored::protoc_bin_path().expect("vendored protoc should be available"),
		);
	}

	tonic_build::configure()
		.compile_protos(&["src/grpc/proto/midnight_indexer.proto"], &["src/grpc/proto"])
		.unwrap();
}
