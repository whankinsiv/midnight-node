fn main() {
	tonic_build::configure()
		.compile_protos(&["proto/midnight_state.proto"], &["proto"])
		.unwrap();
}
