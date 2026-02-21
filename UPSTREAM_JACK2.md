# Upstream Mapping (jack-2)

This repo vendors selected source-of-truth code from:

- `/Users/tosi-n/Documents/Dev/Jenesys/jack-2`

Status at time of initial sync:

- `jack-2` git: **no commits yet** (no `HEAD` SHA available)
- `jack-2` branch (from `git status -sb`): `feature/billing-system-latest`
- Synced at: `2026-02-17`

## Files Vendored

Copied into `vendor/jack2/...` as reference:

- `backend/app/services/banking/bank_statement.py`
- `backend/app/core/enums/banking/bank_statement.py`
- `backend/app/services/banking/conversion/docuclipper.py` (reference only; DocuClipper is not used by default)

## Sync Policy

1. Change source-of-truth behavior in `jack-2` first.
2. Re-sync this repo with `./scripts/sync_from_jack2.sh`.
3. Keep new code here limited to glue and the replacement conversion engine (Docling + HybrIE).

