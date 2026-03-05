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

use crate::grpc::conversions::observed_utxo_from_event;
use crate::midnight_state::{
	BlockByHashRequest, UtxoEventsRequest, midnight_state_client::MidnightStateClient,
};
use midnight_primitives_cnight_observation::{CardanoPosition, ObservedUtxo, TimestampUnixMillis};
use sidechain_domain::*;
use tonic::Status;
use tonic::transport::Channel;

pub async fn get_utxo_events(
	client: &mut MidnightStateClient<Channel>,
	cardano_network: u8,
	start_block: u32,
	start_tx_index: u32,
	tx_capacity: usize,
) -> Result<Vec<ObservedUtxo>, Status> {
	let tx_capacity = u32::try_from(tx_capacity)
		.map_err(|_| tonic::Status::invalid_argument("utxo_capacity too large"))?;

	let response = client
		.get_utxo_events(UtxoEventsRequest { start_block, start_tx_index, tx_capacity })
		.await?
		.into_inner();

	response
		.events
		.into_iter()
		.map(|e| observed_utxo_from_event(e, cardano_network))
		.collect()
}

pub(crate) async fn get_position_by_hash(
	client: &mut MidnightStateClient<Channel>,
	block_hash: McBlockHash,
) -> Result<CardanoPosition, Status> {
	let response = client
		.get_block_by_hash(BlockByHashRequest { block_hash: block_hash.0.to_vec() })
		.await?
		.into_inner();

	Ok(CardanoPosition {
		block_hash,
		block_number: response.block_number as u32,
		block_timestamp: TimestampUnixMillis(response.block_timestamp_unix * 1000),
		tx_index_in_block: response.tx_count,
	})
}
