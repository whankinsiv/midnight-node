# Align cNIGHT UtxoEventsRequest proto tags

Aligns `UtxoEventsRequest` field numbers with the current Acropolis wire
contract and stops sending the removed redundant start fields so cNIGHT gRPC
queries decode correctly.

PR: https://github.com/whankinsiv/midnight-node-acropolis/pull/21
