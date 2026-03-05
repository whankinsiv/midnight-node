use crate::{
	MidnightCNightObservationDataSourceImpl,
	midnight_state::{UtxoEvent, utxo_event},
};
use cardano_serialization_lib::{PlutusData, RewardAddress};
use midnight_primitives_cnight_observation::{
	CardanoPosition, CardanoRewardAddressBytes, CreateData, DeregistrationData, ObservedUtxo,
	ObservedUtxoData, ObservedUtxoHeader, RegistrationData, SpendData, TimestampUnixMillis,
	UtxoIndexInTx,
};
use sidechain_domain::{McBlockHash, McTxHash};
use std::convert::TryFrom;
use tonic::Status;

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
				data: ObservedUtxoData::Registration(RegistrationData {
					cardano_reward_address: CardanoRewardAddressBytes(reward_bytes),
					dust_public_key,
				}),
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

#[allow(clippy::result_large_err)]
fn hash32(bytes: Vec<u8>) -> Result<[u8; 32], Status> {
	<[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| Status::internal("invalid hash length"))
}
