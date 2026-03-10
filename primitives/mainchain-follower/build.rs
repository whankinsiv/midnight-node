fn main() {
	let protoc = protoc_bin_vendored::protoc_bin_path().expect("failed to find vendored protoc");
	unsafe {
		std::env::set_var("PROTOC", protoc);
	}

	tonic_build::configure()
		.compile_protos(&["proto/midnight_state.proto"], &["proto"])
		.unwrap();
}
