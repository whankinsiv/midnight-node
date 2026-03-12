pub mod client;
mod conversions;
pub mod requests;
pub mod midnight_state {
	tonic::include_proto!("midnight_state");
}
