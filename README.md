Beam node sync prototype

This tool performs a trusted-node Beam sync in pure Python and persists the
derived regular-output state in SQLite.

Current scope:
- Single staged pipeline that stores raw headers and block body payloads in SQLite,
	then replays them locally into the derived UTXO state.
- Inclusive `--start-height` and `--stop-height` bounds for resumed work.
- Optional `--fast-sync` mode that requests sparse historical bodies during the
	staging phase using Beam's `GetBodyPack` horizons, then replays them locally.
- Regular outputs only.
- State is persisted in SQLite so reruns resume from the last staged and applied blocks.

Current limitations:
- Trusted node model: no PoW, proof, or chainwork validation beyond simple linear checks.
- `--fast-sync` is UTXO-oriented, not a full reimplementation of Beam node cryptographic fast-sync verification.
- No full reorg handling.
- No shielded output tracking.
- No wallet-readable confidential amounts.
- Treasury bootstrap depends on the connected node exposing the raw treasury payload.

Implementation notes:
- The local state store treats outputs as a multiset, not a unique-by-commitment map. Beam can carry duplicate commitments, so spends resolve against the newest matching unspent entry.
- The staging phase stores canonical headers plus raw `Body` or `BodyPack` payloads so the derive phase can replay them locally without talking to a node again.
- The staging phase also stores the raw treasury payload when the node exposes one, and the derive phase seeds treasury outputs from it before replay.
- The staging phase uses the requested node for both canonical headers and raw block payloads, while still resuming from any already staged headers or block bodies in SQLite.
- Historical mainnet header hashing uses a baked-in rules-hash table because current peers typically advertise only the active fork hash in their Login payload.
- In `--fast-sync` mode, historical perishable bodies are requested as Recovery1 payloads with Beam-equivalent horizons (`Hi=1440`, `Lo=4320`) when the requested range is contiguous from the current derived height and the remaining gap is large enough.
- The CLI always runs both phases; stage-only raw capture and derive-only replay are no longer exposed as separate commands.

Usage:

```bash
python main.py eu-nodes.mainnet.beam.mw:8100 --state-db beam-sync.sqlite3
```

Use the sparse historical fast path:

```bash
python main.py eu-nodes.mainnet.beam.mw:8100 --fast-sync --state-db beam-sync.sqlite3
```

Limit resumed work to an inclusive block range:

```bash
python main.py eu-nodes.mainnet.beam.mw:8100 --state-db beam-sync.sqlite3 --start-height 100000 --stop-height 110000
```

Range semantics:
- `--start-height` and `--stop-height` are inclusive bounds on requested work.
- Earlier requested heights are ignored if the SQLite state already covers them.
- The staged pipeline requires contiguous derived history. On a fresh DB, `--start-height` must be 1; on a resumed DB, the next derivable height is `last_synced_height + 1`.
- `--fast-sync` only applies the sparse Beam-style request path when the requested range is contiguous from the current derived height; otherwise the staging phase falls back to full-body fetching.

Derived output state is stored in the SQLite `outputs` table. Unspent rows keep
`spent_height` as `NULL`, while spent rows retain their creation metadata for
later inspection.
