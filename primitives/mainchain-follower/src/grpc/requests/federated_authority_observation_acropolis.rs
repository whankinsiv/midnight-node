use crate::midnight_state::{
	BlockByHashRequest, CouncilDatumRequest, TechnicalCommitteeDatumRequest,
	midnight_state_client::MidnightStateClient,
};
use sidechain_domain::McBlockHash;
use tonic::Status;
use tonic::transport::Channel;

pub(crate) async fn get_block_number_by_hash(
	client: &mut MidnightStateClient<Channel>,
	block_hash: McBlockHash,
) -> Result<u32, Status> {
	let response = client
		.get_block_by_hash(BlockByHashRequest { block_hash: block_hash.0.to_vec() })
		.await?
		.into_inner();

	Ok(response.block_number)
}

pub(crate) async fn get_council_datum(
	client: &mut MidnightStateClient<Channel>,
	block_number: u32,
) -> Result<Vec<u8>, Status> {
	let response = client
		.get_council_datum(CouncilDatumRequest { block_number: u64::from(block_number) })
		.await?
		.into_inner();

	Ok(response.datum)
}

pub(crate) async fn get_technical_committee_datum(
	client: &mut MidnightStateClient<Channel>,
	block_number: u32,
) -> Result<Vec<u8>, Status> {
	let response = client
		.get_technical_committee_datum(TechnicalCommitteeDatumRequest {
			block_number: u64::from(block_number),
		})
		.await?
		.into_inner();

	Ok(response.datum)
}
