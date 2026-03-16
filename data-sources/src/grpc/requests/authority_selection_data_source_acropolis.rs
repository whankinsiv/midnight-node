use std::collections::HashMap;

use crate::grpc::{
	conversions::{get_stake_delegation, make_stake_map},
	midnight_state::{
		AriadneParametersRequest, EpochCandidatesRequest, EpochNonceRequest,
		midnight_state_client::MidnightStateClient,
	},
};
use cardano_serialization_lib::PlutusData;
use partner_chains_plutus_data::permissioned_candidates::PermissionedCandidateDatums;
use sidechain_domain::{
	CandidateRegistrations, EpochNonce, McEpochNumber, PermissionedCandidateData,
	StakePoolPublicKey,
};
use tonic::{Status, transport::Channel};

pub async fn get_permissioned_candidates(
	client: &mut MidnightStateClient<Channel>,
	epoch: McEpochNumber,
) -> Result<Option<Vec<PermissionedCandidateData>>, Status> {
	let response = client
		.get_ariadne_parameters(AriadneParametersRequest { epoch: epoch.0 as u64 })
		.await?
		.into_inner();

	if response.datum.is_empty() {
		Ok(None)
	} else {
		let datum = PlutusData::from_bytes(response.datum)
			.map_err(|e| Status::internal(format!("failed to decode Ariadne datum CBOR: {e}")))?;
		let datums = PermissionedCandidateDatums::try_from(datum)
			.map_err(|e| Status::internal(format!("failed to decode Ariadne parameters: {e}")))?;

		Ok(Some(datums.into()))
	}
}

pub async fn get_candidates(
	client: &mut MidnightStateClient<Channel>,
	epoch: McEpochNumber,
) -> Result<Vec<CandidateRegistrations>, Status> {
	let response = client
		.get_epoch_candidates(EpochCandidatesRequest { epoch: epoch.0 as u64 })
		.await?
		.into_inner();

	let stake_map = make_stake_map(response.stake_distribution)
		.map_err(|e| Status::internal(format!("candidate conversion failed: {e:?}")))?;

	let mut grouped: HashMap<StakePoolPublicKey, Vec<sidechain_domain::RegistrationData>> =
		HashMap::new();

	for candidate in response.candidates {
		let (pool_key, registration): (StakePoolPublicKey, sidechain_domain::RegistrationData) =
			candidate
				.try_into()
				.map_err(|e| Status::internal(format!("candidate conversion failed: {e:?}")))?;

		grouped.entry(pool_key).or_default().push(registration);
	}

	Ok(grouped
		.into_iter()
		.map(|(stake_pool_public_key, registrations)| {
			let stake_delegation = get_stake_delegation(&stake_map, &stake_pool_public_key);

			CandidateRegistrations { stake_pool_public_key, registrations, stake_delegation }
		})
		.collect())
}

pub async fn get_epoch_nonce(
	client: &mut MidnightStateClient<Channel>,
	epoch: McEpochNumber,
) -> Result<Option<EpochNonce>, Status> {
	let response = client
		.get_epoch_nonce(EpochNonceRequest { epoch: epoch.0 as u64 })
		.await?
		.into_inner();

	Ok(response.nonce.map(EpochNonce))
}
