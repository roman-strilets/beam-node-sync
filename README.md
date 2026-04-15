Beam node sync prototype

This tool performs a trusted-node Beam sync in pure Python and exports the
current unspent regular outputs as JSON lines.

Current scope:
- Direct sync from block 1 to the node tip or a configured stop height.
- Optional staged workflow that stores raw headers and block body payloads in SQLite first.
- Separate derive pass that replays staged blocks into the UTXO state and exports unspent outputs.
- Inclusive `--start-height` and `--stop-height` bounds for resumed work.
- Optional `--fast-sync` mode that requests sparse historical bodies using Beam's `GetBodyPack` horizons, then replays them locally.
- Regular outputs only.
- Exported records include commitment and chain-visible metadata.
- State is persisted in SQLite so reruns resume from the last applied block.

Current limitations:
- Trusted node model: no PoW, proof, or chainwork validation beyond simple linear checks.
- `--fast-sync` is UTXO-oriented, not a full reimplementation of Beam node cryptographic fast-sync verification.
- No full reorg handling.
- No shielded output tracking.
- No wallet-readable confidential amounts.
- Treasury outputs are not reconstructed yet; unresolved early spends are recorded when seen.

Implementation notes:
- The local state store treats outputs as a multiset, not a unique-by-commitment map. Beam can carry duplicate commitments, so spends resolve against the newest matching unspent entry.
- Staged fetch mode stores canonical headers plus raw `Body` or `BodyPack` payloads so later derivation can run locally without talking to a node again.
- Stage mode now uses the requested node for both canonical headers and raw block payloads, while still resuming from any already staged headers or block bodies in SQLite.
- Historical mainnet header hashing uses a baked-in rules-hash table because current peers typically advertise only the active fork hash in their Login payload.
- In `--fast-sync` mode, historical perishable bodies are requested as Recovery1 payloads with Beam-equivalent horizons (`Hi=1440`, `Lo=4320`) when the requested range is contiguous from the current derived height and the remaining gap is large enough.

Usage:

```bash
python main.py eu-nodes.mainnet.beam.mw:8100 --state-db beam-sync.sqlite3 --output utxos.jsonl
```

Use the sparse historical fast path:

```bash
python main.py eu-nodes.mainnet.beam.mw:8100 --fast-sync --state-db beam-sync.sqlite3 --output utxos.jsonl
```

Stage raw blocks first, then derive later:

```bash
python main.py eu-nodes.mainnet.beam.mw:8100 --mode stage --state-db beam-sync.sqlite3
python main.py --mode derive --state-db beam-sync.sqlite3 --output utxos.jsonl
```

Limit resumed work to an inclusive block range:

```bash
python main.py eu-nodes.mainnet.beam.mw:8100 --state-db beam-sync.sqlite3 --start-height 100000 --stop-height 110000 --output utxos.jsonl
python main.py eu-nodes.mainnet.beam.mw:8100 --mode stage --state-db beam-sync.sqlite3 --start-height 100000 --stop-height 110000
python main.py --mode derive --state-db beam-sync.sqlite3 --start-height 100000 --stop-height 110000 --output utxos.jsonl
```

Run the staged pipeline in one command:

```bash
python main.py eu-nodes.mainnet.beam.mw:8100 --mode staged --state-db beam-sync.sqlite3 --output utxos.jsonl
```

Range semantics:
- `--start-height` and `--stop-height` are inclusive bounds on requested work.
- Earlier requested heights are ignored if the SQLite state already covers them.
- `--mode stage` can populate a fresh DB with a sparse raw block range such as `--start-height 1000 --stop-height 2000`.
- Direct sync and derive still require contiguous history from height 1, so a sparse staged DB is useful for raw block capture only until earlier blocks are added.
- `--fast-sync` only applies the sparse Beam-style request path when the requested range is contiguous from the current derived height; otherwise stage/direct fall back to full-body fetching.

The output JSONL contains one unspent output per line, ordered by creation height.
