// This file is part of midnight-node.
// Copyright (C) 2025-2026 Midnight Foundation
// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::MidnightCNightObservationDataSourceImpl;
use crate::midnight_state::{
	AssetCreatesRequest, AssetSpendsRequest, BlockByHashRequest, DeregistrationsRequest,
	RegistrationsRequest, midnight_state_client::MidnightStateClient,
};
use cardano_serialization_lib::{PlutusData, RewardAddress};
use midnight_primitives_cnight_observation::{
	CardanoPosition, CardanoRewardAddressBytes, CreateData, DeregistrationData, ObservedUtxo,
	ObservedUtxoData, ObservedUtxoHeader, SpendData, TimestampUnixMillis, UtxoIndexInTx,
};
use sidechain_domain::*;
use tonic::transport::Channel;

#[allow(clippy::too_many_arguments)]
pub async fn get_registrations(
	client: &mut MidnightStateClient<Channel>,
	cardano_network: u8,
	start_block: u32,
	end_block: u32,
) -> Result<Vec<ObservedUtxo>, tonic::Status> {
	let response = client
		.get_registrations(RegistrationsRequest {
			start_block: start_block as u64,
			end_block: end_block as u64,
		})
		.await?
		.into_inner();

	response
		.registrations
		.into_iter()
		.map(|c| {
			let datum = PlutusData::from_bytes(c.full_datum)
				.map_err(|e| tonic::Status::internal(format!("Invalid CBOR datum: {e}")))?;

			let constr = datum
				.as_constr_plutus_data()
				.ok_or_else(|| tonic::Status::internal("Registration datum not Constr"))?;

			let (credential, dust_public_key) =
				MidnightCNightObservationDataSourceImpl::decode_registration_datum(constr)
					.map_err(|e| tonic::Status::internal(format!("Datum decode error: {e}")))?;

			let reward_address = RewardAddress::new(cardano_network, &credential);
			// Unwrap here is OK - we know the reward_address is always 29 bytes
			let cardano_address = reward_address.to_address().to_bytes().try_into().unwrap();

			let block_hash: [u8; 32] = c
				.block_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid block hash length"))?;

			let tx_hash: [u8; 32] = c
				.tx_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid tx hash length"))?;

			Ok(ObservedUtxo {
				header: ObservedUtxoHeader {
					tx_position: CardanoPosition {
						block_hash: McBlockHash(block_hash),
						block_number: c.block_number as u32,
						block_timestamp: TimestampUnixMillis(c.block_timestamp_unix * 1000),
						tx_index_in_block: c.tx_index,
					},
					tx_hash: McTxHash(tx_hash),
					utxo_tx_hash: McTxHash(tx_hash),
					utxo_index: UtxoIndexInTx(c.output_index as u16),
				},
				data: ObservedUtxoData::Registration(
					midnight_primitives_cnight_observation::RegistrationData {
						cardano_reward_address: CardanoRewardAddressBytes(cardano_address),
						dust_public_key,
					},
				),
			})
		})
		.collect::<Result<Vec<_>, tonic::Status>>()
}

pub async fn get_deregistrations(
	client: &mut MidnightStateClient<Channel>,
	cardano_network: u8,
	start_block: u32,
	end_block: u32,
) -> Result<Vec<ObservedUtxo>, tonic::Status> {
	let response = client
		.get_deregistrations(DeregistrationsRequest {
			start_block: start_block as u64,
			end_block: end_block as u64,
		})
		.await?
		.into_inner();

	response
		.deregistrations
		.into_iter()
		.map(|c| {
			let datum = PlutusData::from_bytes(c.full_datum)
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

			let block_hash: [u8; 32] = c
				.block_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid block hash length"))?;

			let tx_hash: [u8; 32] = c
				.tx_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid tx hash length"))?;

			let utxo_tx_hash: [u8; 32] = c
				.utxo_tx_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid utxo tx hash length"))?;

			Ok(ObservedUtxo {
				header: ObservedUtxoHeader {
					tx_position: CardanoPosition {
						block_hash: McBlockHash(block_hash),
						block_number: c.block_number as u32,
						block_timestamp: TimestampUnixMillis(c.block_timestamp_unix * 1000),
						tx_index_in_block: c.tx_index,
					},
					tx_hash: McTxHash(tx_hash),
					utxo_tx_hash: McTxHash(utxo_tx_hash),
					utxo_index: UtxoIndexInTx(c.utxo_index as u16),
				},
				data: ObservedUtxoData::Deregistration(DeregistrationData {
					cardano_reward_address: CardanoRewardAddressBytes(reward_bytes),
					dust_public_key,
				}),
			})
		})
		.collect::<Result<Vec<_>, tonic::Status>>()
}

pub(crate) async fn get_asset_creates(
	client: &mut MidnightStateClient<Channel>,
	start_block: u32,
	end_block: u32,
) -> Result<Vec<ObservedUtxo>, tonic::Status> {
	let response = client
		.get_asset_creates(AssetCreatesRequest {
			start_block: start_block as u64,
			end_block: end_block as u64,
		})
		.await?
		.into_inner();

	response
		.creates
		.into_iter()
		.map(|c| {
			let block_hash: [u8; 32] = c
				.block_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid block hash length"))?;

			let tx_hash: [u8; 32] = c
				.tx_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid tx hash length"))?;

			let address: [u8; 29] = c
				.address
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid address length"))?;

			Ok(ObservedUtxo {
				header: ObservedUtxoHeader {
					tx_position: CardanoPosition {
						block_hash: McBlockHash(block_hash),
						block_number: c.block_number as u32,
						block_timestamp: TimestampUnixMillis(c.block_timestamp_unix * 1000),
						tx_index_in_block: c.tx_index,
					},
					tx_hash: McTxHash(tx_hash),
					utxo_tx_hash: McTxHash(tx_hash),
					utxo_index: UtxoIndexInTx(c.output_index as u16),
				},
				data: ObservedUtxoData::AssetCreate(CreateData {
					value: c.quantity as u128,
					owner: CardanoRewardAddressBytes(address),
					utxo_tx_hash: McTxHash(tx_hash),
					utxo_tx_index: c.output_index as u16,
				}),
			})
		})
		.collect::<Result<Vec<_>, tonic::Status>>()
}

pub(crate) async fn get_asset_spends(
	client: &mut MidnightStateClient<Channel>,
	start_block: u32,
	end_block: u32,
) -> Result<Vec<ObservedUtxo>, tonic::Status> {
	let response = client
		.get_asset_spends(AssetSpendsRequest {
			start_block: start_block as u64,
			end_block: end_block as u64,
		})
		.await?
		.into_inner();

	response
		.spends
		.into_iter()
		.map(|c| {
			let block_hash: [u8; 32] = c
				.block_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid block hash length"))?;

			let utxo_tx_hash: [u8; 32] = c
				.utxo_tx_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid utxo tx hash length"))?;

			let spending_tx_hash: [u8; 32] = c
				.spending_tx_hash
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid spending tx hash length"))?;

			let address: [u8; 29] = c
				.address
				.try_into()
				.map_err(|_| tonic::Status::internal("Invalid address length"))?;

			Ok(ObservedUtxo {
				header: ObservedUtxoHeader {
					tx_position: CardanoPosition {
						block_hash: McBlockHash(block_hash),
						block_number: c.block_number as u32,
						block_timestamp: TimestampUnixMillis(c.block_timestamp_unix * 1000),
						tx_index_in_block: c.tx_index,
					},
					tx_hash: McTxHash(spending_tx_hash),
					utxo_tx_hash: McTxHash(utxo_tx_hash),
					utxo_index: UtxoIndexInTx(c.utxo_index as u16),
				},
				data: ObservedUtxoData::AssetSpend(SpendData {
					value: c.quantity as u128,
					owner: CardanoRewardAddressBytes(address),
					utxo_tx_hash: McTxHash(utxo_tx_hash),
					utxo_tx_index: c.utxo_index as u16,
					spending_tx_hash: McTxHash(spending_tx_hash),
				}),
			})
		})
		.collect::<Result<Vec<_>, tonic::Status>>()
}

pub(crate) async fn get_block_number_by_hash(
	client: &mut MidnightStateClient<Channel>,
	block_hash: McBlockHash,
) -> Result<u32, tonic::Status> {
	let response = client
		.get_block_by_hash(BlockByHashRequest { block_hash: block_hash.0.to_vec() })
		.await?
		.into_inner();

	Ok(response.block_number as u32)
}
