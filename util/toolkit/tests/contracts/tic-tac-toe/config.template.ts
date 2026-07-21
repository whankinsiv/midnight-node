import { CompiledContract, ContractExecutable } from '@midnight-ntwrk/compact-js/effect';
import { Contract as TicTacToeContract_ } from './out/contract/index.js';

// The contract declares no witnesses and keeps all state on-chain, so the private state is empty.
type TicTacToePrivateState = Record<string, never>;

type TicTacToeContract = TicTacToeContract_<TicTacToePrivateState>;
const TicTacToeContract = TicTacToeContract_;

const createInitialPrivateState: () => TicTacToePrivateState = () => ({});

export default {
  contractExecutable: CompiledContract.make<TicTacToeContract>(
    'TicTacToeContract',
    TicTacToeContract,
  ).pipe(
    CompiledContract.withVacantWitnesses,
    CompiledContract.withCompiledFileAssets('./out'),
    ContractExecutable.make,
  ),
  createInitialPrivateState,
  config: {
    keys: {
      coinPublic: '{{COIN_PUBLIC}}',
    },
    network: '{{NETWORK}}',
  },
};
