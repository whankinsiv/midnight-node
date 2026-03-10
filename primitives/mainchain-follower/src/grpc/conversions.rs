use crate::{
	MidnightCNightObservationDataSourceImpl,
	midnight_state::{self, EpochCandidate, StakePoolEntry, UtxoEvent, utxo_event},
};
use cardano_serialization_lib::{PlutusData, RewardAddress};
use midnight_primitives_cnight_observation::{
	CardanoPosition, CardanoRewardAddressBytes, CreateData, DeregistrationData, ObservedUtxo,
	ObservedUtxoData, ObservedUtxoHeader, SpendData, TimestampUnixMillis, UtxoIndexInTx,
};
use partner_chains_plutus_data::registered_candidates::RegisterValidatorDatum;
use sidechain_domain::{
	CandidateKeys, CrossChainPublicKey, CrossChainSignature, MainchainKeyHash, McBlockHash,
	McTxHash, StakeDelegation, StakePoolPublicKey, UtxoId, UtxoIndex, UtxoInfo,
};
use std::{collections::HashMap, convert::TryFrom};
use tonic::Status;

#[derive(Debug)]
pub enum CandidateConversionError {
	MissingStakeKey,
	InvalidDatum,
	InvalidHashLength,
	InvalidIndex,
}

#[allow(clippy::result_large_err)]
pub fn observed_utxo_from_event(
	event: UtxoEvent,
	cardano_network: u8,
) -> Result<ObservedUtxo, tonic::Status> {
	let kind = event.kind.ok_or_else(|| Status::internal("missing utxo event kind"))?;

	match kind {
		utxo_event::Kind::AssetCreate(e) => {
			let header = ObservedUtxoHeader {
				tx_position: CardanoPosition {
					block_hash: McBlockHash(hash32(e.block_hash)?),
					block_number: e.block_number as u32,
					block_timestamp: TimestampUnixMillis(e.block_timestamp_unix * 1000),
					tx_index_in_block: e.tx_index,
				},
				tx_hash: McTxHash(hash32(e.tx_hash.clone())?),
				utxo_tx_hash: McTxHash(hash32(e.tx_hash.clone())?),
				utxo_index: UtxoIndexInTx(e.output_index as u16),
			};

			Ok(ObservedUtxo {
				header,
				data: ObservedUtxoData::AssetCreate(CreateData {
					owner: e
						.address
						.try_into()
						.map_err(|_| tonic::Status::internal("Invalid address length"))?,
					value: e.quantity as u128,
					utxo_tx_hash: e
						.tx_hash
						.try_into()
						.map_err(|_| tonic::Status::internal("Invalid tx hash length"))?,
					utxo_tx_index: e.tx_index as u16,
				}),
			})
		},

		utxo_event::Kind::AssetSpend(e) => {
			let header = ObservedUtxoHeader {
				tx_position: CardanoPosition {
					block_hash: McBlockHash(hash32(e.block_hash)?),
					block_number: e.block_number as u32,
					block_timestamp: TimestampUnixMillis(e.block_timestamp_unix * 1000),
					tx_index_in_block: e.tx_index,
				},
				tx_hash: McTxHash(hash32(e.spending_tx_hash.clone())?),
				utxo_tx_hash: McTxHash(hash32(e.utxo_tx_hash.clone())?),
				utxo_index: UtxoIndexInTx(e.utxo_index as u16),
			};

			Ok(ObservedUtxo {
				header,
				data: ObservedUtxoData::AssetSpend(SpendData {
					value: e.quantity as u128,
					owner: e
						.address
						.try_into()
						.map_err(|_| tonic::Status::internal("Invalid address length"))?,
					utxo_tx_hash: McTxHash(hash32(e.utxo_tx_hash)?),
					utxo_tx_index: e.utxo_index as u16,
					spending_tx_hash: McTxHash(hash32(e.spending_tx_hash)?),
				}),
			})
		},

		utxo_event::Kind::Registration(e) => {
			let header = ObservedUtxoHeader {
				tx_position: CardanoPosition {
					block_hash: McBlockHash(hash32(e.block_hash)?),
					block_number: e.block_number as u32,
					block_timestamp: TimestampUnixMillis(e.block_timestamp_unix),
					tx_index_in_block: e.tx_index,
				},
				tx_hash: McTxHash(hash32(e.tx_hash.clone())?),
				utxo_tx_hash: McTxHash(hash32(e.tx_hash)?),
				utxo_index: UtxoIndexInTx(e.output_index as u16),
			};

			let datum = PlutusData::from_bytes(e.full_datum)
				.map_err(|e| tonic::Status::internal(format!("Invalid CBOR datum: {e}")))?;

			let constr = datum
				.as_constr_plutus_data()
				.ok_or_else(|| tonic::Status::internal("Deregistration datum not Constr"))?;

			let (credential, dust_public_key) =
				MidnightCNightObservationDataSourceImpl::decode_registration_datum(constr)
					.map_err(|e| tonic::Status::internal(format!("Datum decode error: {e}")))?;

			let reward_address = RewardAddress::new(cardano_network, &credential);

			let reward_bytes: [u8; 29] = reward_address
				.to_address()
				.to_bytes()
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid reward address length"))?;

			Ok(ObservedUtxo {
				header,
				data: ObservedUtxoData::Registration(
					midnight_primitives_cnight_observation::RegistrationData {
						cardano_reward_address: CardanoRewardAddressBytes(reward_bytes),
						dust_public_key,
					},
				),
			})
		},

		utxo_event::Kind::Deregistration(e) => {
			let header = ObservedUtxoHeader {
				tx_position: CardanoPosition {
					block_hash: McBlockHash(hash32(e.block_hash)?),
					block_number: e.block_number as u32,
					block_timestamp: TimestampUnixMillis(e.block_timestamp_unix),
					tx_index_in_block: e.tx_index,
				},
				tx_hash: McTxHash(hash32(e.tx_hash.clone())?),
				utxo_tx_hash: McTxHash(hash32(e.tx_hash)?),
				utxo_index: UtxoIndexInTx(e.utxo_index as u16),
			};

			let datum = PlutusData::from_bytes(e.full_datum)
				.map_err(|e| tonic::Status::internal(format!("Invalid CBOR datum: {e}")))?;

			let constr = datum
				.as_constr_plutus_data()
				.ok_or_else(|| tonic::Status::internal("Deregistration datum not Constr"))?;

			let (credential, dust_public_key) =
				MidnightCNightObservationDataSourceImpl::decode_registration_datum(constr)
					.map_err(|e| tonic::Status::internal(format!("Datum decode error: {e}")))?;

			let reward_address = RewardAddress::new(cardano_network, &credential);

			let reward_bytes: [u8; 29] = reward_address
				.to_address()
				.to_bytes()
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid reward address length"))?;

			Ok(ObservedUtxo {
				header,
				data: ObservedUtxoData::Deregistration(DeregistrationData {
					cardano_reward_address: CardanoRewardAddressBytes(reward_bytes),
					dust_public_key,
				}),
			})
		},
	}
}

impl TryFrom<EpochCandidate> for (StakePoolPublicKey, sidechain_domain::RegistrationData) {
	type Error = CandidateConversionError;

	fn try_from(value: EpochCandidate) -> Result<Self, Self::Error> {
		let utxo_info = UtxoInfo::try_from(&value)?;

		let tx_inputs = value
			.tx_inputs
			.into_iter()
			.map(UtxoId::try_from)
			.collect::<Result<Vec<_>, _>>()?;

		let datum = RegisterValidatorDatum::try_from(PlutusData::new_bytes(value.full_datum))
			.map_err(|_| CandidateConversionError::InvalidDatum)?;

		match datum {
			RegisterValidatorDatum::V0 {
				stake_ownership,
				sidechain_pub_key,
				sidechain_signature,
				registration_utxo,
				own_pkh: _own_pkh,
				aura_pub_key,
				grandpa_pub_key,
			} => Ok((
				stake_ownership.pub_key,
				sidechain_domain::RegistrationData {
					mainchain_signature: stake_ownership.signature,
					// For now we use the same key for both cross chain and sidechain actions
					cross_chain_pub_key: CrossChainPublicKey(sidechain_pub_key.0.clone()),
					cross_chain_signature: CrossChainSignature(sidechain_signature.0.clone()),
					sidechain_signature,
					sidechain_pub_key,
					keys: CandidateKeys(vec![aura_pub_key.into(), grandpa_pub_key.into()]),
					registration_utxo,
					tx_inputs,
					utxo_info,
				},
			)),
			RegisterValidatorDatum::V1 {
				stake_ownership,
				sidechain_pub_key,
				sidechain_signature,
				registration_utxo,
				own_pkh: _own_pkh,
				keys,
			} => Ok((
				stake_ownership.pub_key,
				sidechain_domain::RegistrationData {
					mainchain_signature: stake_ownership.signature,
					// For now we use the same key for both cross chain and sidechain actions
					cross_chain_pub_key: CrossChainPublicKey(sidechain_pub_key.0.clone()),
					cross_chain_signature: CrossChainSignature(sidechain_signature.0.clone()),
					sidechain_signature,
					sidechain_pub_key,
					keys,
					registration_utxo,
					tx_inputs,
					utxo_info,
				},
			)),
		}
	}
}

impl TryFrom<midnight_state::UtxoId> for UtxoId {
	type Error = CandidateConversionError;

	fn try_from(value: midnight_state::UtxoId) -> Result<Self, Self::Error> {
		let tx_hash = value
			.tx_hash
			.try_into()
			.map_err(|_| CandidateConversionError::InvalidHashLength)?;

		let index = value.index.try_into().map_err(|_| CandidateConversionError::InvalidIndex)?;

		Ok(Self { tx_hash: McTxHash(tx_hash), index: UtxoIndex(index) })
	}
}

impl TryFrom<&EpochCandidate> for UtxoInfo {
	type Error = CandidateConversionError;

	fn try_from(value: &EpochCandidate) -> Result<Self, Self::Error> {
		let tx_hash: [u8; 32] = value
			.utxo_tx_hash
			.as_slice()
			.try_into()
			.map_err(|_| CandidateConversionError::InvalidHashLength)?;

		let index = value
			.utxo_index
			.try_into()
			.map_err(|_| CandidateConversionError::InvalidIndex)?;

		let epoch_number = sidechain_domain::McEpochNumber(
			value
				.epoch_number
				.try_into()
				.map_err(|_| CandidateConversionError::InvalidIndex)?,
		);

		let block_number = sidechain_domain::McBlockNumber(
			value
				.block_number
				.try_into()
				.map_err(|_| CandidateConversionError::InvalidIndex)?,
		);

		Ok(Self {
			utxo_id: UtxoId { tx_hash: McTxHash(tx_hash), index: UtxoIndex(index) },
			epoch_number,
			block_number,
			slot_number: sidechain_domain::McSlotNumber(value.slot_number),
			tx_index_within_block: sidechain_domain::McTxIndexInBlock(value.tx_index),
		})
	}
}

#[allow(clippy::result_large_err)]
fn hash32(bytes: Vec<u8>) -> Result<[u8; 32], Status> {
	<[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| Status::internal("invalid hash length"))
}

pub fn make_stake_map(
	stake_pool_entries: Vec<StakePoolEntry>,
) -> Result<HashMap<MainchainKeyHash, StakeDelegation>, CandidateConversionError> {
	stake_pool_entries
		.into_iter()
		.map(|e| {
			let hash: [u8; 28] = e
				.pool_hash
				.try_into()
				.map_err(|_| CandidateConversionError::InvalidHashLength)?;

			Ok((MainchainKeyHash(hash), StakeDelegation(e.stake)))
		})
		.collect()
}

pub fn get_stake_delegation(
	stake_map: &HashMap<MainchainKeyHash, StakeDelegation>,
	stake_pool_pub_key: &StakePoolPublicKey,
) -> Option<StakeDelegation> {
	if stake_map.is_empty() {
		None
	} else {
		Some(
			stake_map
				.get(&MainchainKeyHash::from_vkey(&stake_pool_pub_key.0))
				.cloned()
				.unwrap_or(StakeDelegation(0)),
		)
	}
}
