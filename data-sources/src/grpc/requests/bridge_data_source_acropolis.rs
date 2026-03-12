// This file is part of midnight-node.
// Copyright (C) 2025-2026 Midnight Foundation
// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use cardano_serialization_lib::PlutusData;
use partner_chains_plutus_data::bridge::{TokenTransferDatum, TokenTransferDatumV1};
use sidechain_domain::{McBlockHash, McBlockNumber, McTxHash, UtxoId, UtxoIndex};
use sp_partner_chains_bridge::{BridgeDataCheckpoint, BridgeTransferV1};
use tonic::{Status, transport::Channel};

use crate::grpc::{
	conversions::hash32,
	midnight_state::{
		BlockByHashRequest, BridgeCheckpoint, BridgeUtxo, BridgeUtxosRequest,
		UtxoId as UtxoIdProto, bridge_checkpoint, bridge_utxos_request,
		midnight_state_client::MidnightStateClient,
	},
};

pub(crate) async fn get_bridge_transfers<RecipientAddress>(
	client: &mut MidnightStateClient<Channel>,
	data_checkpoint: BridgeDataCheckpoint,
	max_transfers: u32,
	current_mc_block_hash: McBlockHash,
) -> Result<(Vec<BridgeTransferV1<RecipientAddress>>, BridgeDataCheckpoint), Status>
where
	RecipientAddress: for<'a> TryFrom<&'a [u8]>,
{
	let to_block = get_block_number_by_hash(client, current_mc_block_hash).await?;

	let response = client
		.get_bridge_utxos(BridgeUtxosRequest {
			checkpoint: Some(checkpoint_to_proto(data_checkpoint)?),
			to_block: u64::from(to_block.0),
			utxo_capacity: max_transfers,
		})
		.await?
		.into_inner();

	let transfers = response
		.utxos
		.into_iter()
		.map(bridge_utxo_to_transfer)
		.collect::<Result<Vec<_>, _>>()?
		.into_iter()
		.flatten()
		.collect();

	let next_checkpoint = checkpoint_from_proto(
		response
			.next_checkpoint
			.ok_or_else(|| Status::internal("BridgeUtxosResponse missing next_checkpoint"))?,
	)?;

	Ok((transfers, next_checkpoint))
}

async fn get_block_number_by_hash(
	client: &mut MidnightStateClient<Channel>,
	block_hash: McBlockHash,
) -> Result<McBlockNumber, Status> {
	let response = client
		.get_block_by_hash(BlockByHashRequest { block_hash: block_hash.0.to_vec() })
		.await?
		.into_inner();

	Ok(McBlockNumber(response.block_number))
}

#[allow(clippy::result_large_err)]
fn checkpoint_to_proto(
	checkpoint: BridgeDataCheckpoint,
) -> Result<bridge_utxos_request::Checkpoint, Status> {
	match checkpoint {
		BridgeDataCheckpoint::Block(block_number) => {
			Ok(bridge_utxos_request::Checkpoint::BlockNumber(u64::from(block_number.0)))
		},
		BridgeDataCheckpoint::Utxo(utxo) => {
			Ok(bridge_utxos_request::Checkpoint::Utxo(UtxoIdProto {
				tx_hash: utxo.tx_hash.0.to_vec(),
				index: u32::from(utxo.index.0),
			}))
		},
	}
}

#[allow(clippy::result_large_err)]
fn checkpoint_from_proto(checkpoint: BridgeCheckpoint) -> Result<BridgeDataCheckpoint, Status> {
	match checkpoint.kind {
		Some(bridge_checkpoint::Kind::BlockNumber(block_number)) => {
			let block_number = u32::try_from(block_number)
				.map_err(|_| Status::internal("bridge checkpoint block number overflow"))?;
			Ok(BridgeDataCheckpoint::Block(McBlockNumber(block_number)))
		},
		Some(bridge_checkpoint::Kind::Utxo(utxo)) => Ok(BridgeDataCheckpoint::Utxo(utxo_id(utxo)?)),
		None => Err(Status::internal("BridgeCheckpoint missing kind")),
	}
}

#[allow(clippy::result_large_err)]
fn bridge_utxo_to_transfer<RecipientAddress>(
	utxo: BridgeUtxo,
) -> Result<Option<BridgeTransferV1<RecipientAddress>>, Status>
where
	RecipientAddress: for<'a> TryFrom<&'a [u8]>,
{
	let Some(token_amount) = utxo.tokens_out.checked_sub(utxo.tokens_in) else {
		return Ok(None);
	};

	if token_amount == 0 {
		return Ok(None);
	}

	let utxo_id = utxo_id(UtxoIdProto { tx_hash: utxo.tx_hash.clone(), index: utxo.output_index })?;

	let Some(datum_bytes) = utxo.datum else {
		return Ok(Some(BridgeTransferV1::InvalidTransfer { token_amount, utxo_id }));
	};

	let datum = match PlutusData::from_bytes(datum_bytes) {
		Ok(datum) => datum,
		Err(_) => return Ok(Some(BridgeTransferV1::InvalidTransfer { token_amount, utxo_id })),
	};

	let transfer = match TokenTransferDatum::try_from(datum) {
		Ok(TokenTransferDatum::V1(TokenTransferDatumV1::UserTransfer { receiver })) => {
			match RecipientAddress::try_from(receiver.0.as_ref()) {
				Ok(recipient) => BridgeTransferV1::UserTransfer { token_amount, recipient },
				Err(_) => BridgeTransferV1::InvalidTransfer { token_amount, utxo_id },
			}
		},
		Ok(TokenTransferDatum::V1(TokenTransferDatumV1::ReserveTransfer)) => {
			BridgeTransferV1::ReserveTransfer { token_amount }
		},
		Err(_) => BridgeTransferV1::InvalidTransfer { token_amount, utxo_id },
	};

	Ok(Some(transfer))
}

#[allow(clippy::result_large_err)]
fn utxo_id(utxo: UtxoIdProto) -> Result<UtxoId, Status> {
	let index =
		u16::try_from(utxo.index).map_err(|_| Status::internal("bridge utxo index overflow"))?;

	Ok(UtxoId { tx_hash: McTxHash(hash32(utxo.tx_hash)?), index: UtxoIndex(index) })
}
