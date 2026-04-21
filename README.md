# Looker Instance Wipe

API-driven teardown script for decommissioning a Looker instance. Executes up to 22 steps via the Looker SDK, writes a timestamped audit log of every action taken, and requires explicit confirmation before making any changes.

## Prerequisites

- Python 3.9+
- `pip install -r requirements.txt` (installs `looker-sdk>=24.0.0`)
- Admin-level API3 credentials for the target Looker instance

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials:
   ```
   LOOKER_BASE_URL=https://yourcompany.looker.com
   LOOKER_CLIENT_ID=your_client_id
   LOOKER_CLIENT_SECRET=your_client_secret
   ```
2. Optionally copy `looker.ini.example` to `looker.ini` to configure SSL/timeout settings.

## Usage

**Dry run** (no changes made — always do this first):
```bash
python3 wipe.py --dry-run
```

**Real run** (requires typing `wipe` to confirm):
```bash
python3 wipe.py
```

**Pass credentials as CLI flags** (alternative to `.env`):
```bash
python3 wipe.py --base-url https://yourcompany.looker.com --client-id ID --client-secret SECRET
```

**Run specific steps only:**
```bash
python3 wipe.py --steps 1,2,6 --dry-run
```

**Custom audit log path:**
```bash
python3 wipe.py --audit-log /path/to/my_audit.json
```

## What It Does

Each step targets a specific resource type. Steps are skipped gracefully if no resources are found or if the API is unavailable.

| Step | Action |
|------|--------|
| 1 | Kill all running queries |
| 2 | Delete all scheduled plans |
| 3 | Delete all alerts |
| 4 | Invalidate OAuth tokens and delete OAuth client apps |
| 5 | Delete all embed secrets |
| 6 | Delete all non-admin users (all admins are protected) |
| 7 | Hard-delete all dashboards (including soft-deleted) |
| 8 | Hard-delete all Looks (including soft-deleted) |
| 9 | Delete all non-system folders |
| 10 | Delete all boards |
| 11 | Delete all database connections |
| 12 | Delete all SSH tunnels and servers |
| 13 | Delete all custom themes (built-in defaults protected) |
| 14 | Delete all custom color collections (built-ins protected) |
| 15 | Delete all user attributes (system attributes protected) |
| 16 | Delete all custom groups (built-in "All Users" protected) |
| 17 | Delete custom roles, permission sets, and model sets (built-ins protected) |
| 18 | Delete all integration hubs |
| 19 | Neutralize OIDC / SAML / LDAP auth configs (overwrite with dummy values) |
| 20 | Flush all query result caches via datagroups |
| 21 | Delete all LookML models |
| 22 | *(Skipped)* Git branch cleanup is handled by the offboarding pipeline |
| 23 | Write audit log and print summary |

## Audit Log

Every run produces a `wipe_audit_<timestamp>.json` file (or the path specified via `--audit-log`). Each entry records the timestamp, step number, action, resource type, resource ID, status (`deleted` / `skipped` / `error` / `dry_run`), and any detail message.

## After the Script

**Phase 2 (manual in Looker UI):** Delete LookML projects, verify Trash is empty, check System Activity.

**Phase 3 (support ticket):** Request deletion attestation and backup purge confirmation from Looker support.

## Notes

- Git dev branches are intentionally excluded. All Looker instances share a single repo, so branch cleanup is scoped per-arena and handled by the offboarding pipeline.
- The script does not push any changes to Git — it only calls the Looker REST API.
- `.env` and `looker.ini` are gitignored and should never be committed.
