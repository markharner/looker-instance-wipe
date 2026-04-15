#!/usr/bin/env python3
"""
Looker Instance Wipe — Phase 1 API-driven teardown
Based on: Looker Instance Wipe Research Report (runbook steps 1–23)

Usage:
    python wipe.py --base-url https://example.looker.com --client-id ID --client-secret SECRET
    python wipe.py --base-url https://example.looker.com --client-id ID --client-secret SECRET --dry-run

The currently authenticated admin user is automatically protected from deletion.
All actions are recorded to an audit log (wipe_audit_<timestamp>.json).
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any

import looker_sdk
from looker_sdk import models40 as models


# ---------------------------------------------------------------------------
# Audit logger
# ---------------------------------------------------------------------------

class AuditLog:
    def __init__(self, path: str):
        self.path = path
        self.entries: list[dict] = []
        self.dry_run = False

    def record(
        self,
        step: int,
        action: str,
        resource_type: str,
        resource_id: Any,
        status: str,
        detail: str = "",
    ):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "step": step,
            "action": action,
            "resource_type": resource_type,
            "resource_id": str(resource_id),
            "status": status,  # "deleted" | "skipped" | "error" | "dry_run"
            "detail": detail,
        }
        self.entries.append(entry)
        mode = "[DRY RUN] " if self.dry_run and status == "dry_run" else ""
        symbol = {"deleted": "✓", "skipped": "–", "error": "✗", "dry_run": "~"}.get(status, "?")
        print(f"  {symbol} {mode}{resource_type} {resource_id}: {status}{' — ' + detail if detail else ''}")

    def save(self):
        with open(self.path, "w") as f:
            json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "entries": self.entries}, f, indent=2)
        print(f"\nAudit log saved → {self.path}")

    def summary(self):
        counts: dict[str, int] = {}
        for e in self.entries:
            counts[e["status"]] = counts.get(e["status"], 0) + 1
        return counts


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def step_header(n: int, title: str):
    print(f"\n[Step {n:02d}] {title}")


def safe_delete(audit: AuditLog, step: int, fn, resource_type: str, resource_id: Any, dry_run: bool, _retries: int = 2, **kwargs):
    """Call fn(resource_id) unless dry_run; log the result.

    Transient network/socket errors (OSError, ConnectionError) are retried up to
    _retries times with a short back-off before being recorded as errors.
    """
    if dry_run:
        audit.record(step, "DELETE", resource_type, resource_id, "dry_run")
        return True

    last_exc = None
    for attempt in range(1, _retries + 2):  # attempts = retries + 1
        try:
            fn(resource_id, **kwargs)
            audit.record(step, "DELETE", resource_type, resource_id, "deleted")
            return True
        except Exception as e:
            msg = str(e)[:120]
            # 404 means already gone — treat as success immediately
            if "404" in msg:
                audit.record(step, "DELETE", resource_type, resource_id, "skipped", "404 already gone")
                return True
            # 405 on built-in objects — expected, skip silently
            if "405" in msg:
                audit.record(step, "DELETE", resource_type, resource_id, "skipped", "405 not deletable (built-in)")
                return True
            # Transient transport errors — retry with back-off
            is_transient = isinstance(e, (OSError, ConnectionError)) or "Connection aborted" in msg or "OSError" in msg
            if is_transient and attempt <= _retries:
                wait = attempt * 2  # 2s, 4s
                print(f"    Transient error on {resource_type} {resource_id} (attempt {attempt}), retrying in {wait}s…")
                time.sleep(wait)
                last_exc = e
                continue
            last_exc = e
            break

    audit.record(step, "DELETE", resource_type, resource_id, "error", str(last_exc)[:120])
    return False


def safe_patch(audit: AuditLog, step: int, fn, resource_type: str, resource_id: Any, dry_run: bool, body):
    if dry_run:
        audit.record(step, "PATCH", resource_type, resource_id, "dry_run")
        return True
    try:
        fn(resource_id, body)
        audit.record(step, "PATCH", resource_type, resource_id, "deleted", "overwritten with dummy values")
        return True
    except Exception as e:
        audit.record(step, "PATCH", resource_type, resource_id, "error", str(e)[:120])
        return False


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------

def step_01_kill_running_queries(sdk, audit, dry_run):
    step_header(1, "Kill all running queries")
    try:
        queries = sdk.all_running_queries()
    except Exception as e:
        print(f"  Could not list running queries: {e}")
        return
    for q in queries:
        safe_delete(audit, 1, sdk.kill_query, "running_query", q.id, dry_run)


def step_02_delete_scheduled_plans(sdk, audit, dry_run):
    step_header(2, "Delete all scheduled plans")
    try:
        plans = sdk.all_scheduled_plans(all_users=True)
    except Exception as e:
        print(f"  Could not list scheduled plans: {e}")
        return
    for p in plans:
        safe_delete(audit, 2, sdk.delete_scheduled_plan, "scheduled_plan", p.id, dry_run)


def step_03_delete_alerts(sdk, audit, dry_run):
    step_header(3, "Delete all alerts")
    try:
        alerts = sdk.search_alerts()
    except Exception as e:
        print(f"  Could not list alerts: {e}")
        return
    for a in alerts:
        safe_delete(audit, 3, sdk.delete_alert, "alert", a.id, dry_run)


def step_04_delete_oauth_apps(sdk, audit, dry_run):
    step_header(4, "Invalidate OAuth tokens and delete OAuth client apps")
    try:
        apps = sdk.all_oauth_client_apps()
    except Exception as e:
        print(f"  Could not list OAuth apps: {e}")
        return
    for app in apps:
        # Invalidate tokens first
        if not dry_run:
            try:
                sdk.invalidate_tokens(app.client_guid)
                audit.record(4, "DELETE", "oauth_tokens", app.client_guid, "deleted", "tokens invalidated")
            except Exception as e:
                audit.record(4, "DELETE", "oauth_tokens", app.client_guid, "error", str(e)[:120])
        else:
            audit.record(4, "DELETE", "oauth_tokens", app.client_guid, "dry_run")
        # Delete the app
        safe_delete(audit, 4, sdk.delete_oauth_client_app, "oauth_client_app", app.client_guid, dry_run)


def step_05_delete_embed_secrets(sdk, audit, dry_run):
    step_header(5, "Delete all embed secrets")
    # The embed SSO secret endpoint varies across SDK versions.
    # Try the available methods; skip gracefully if none exist.
    try:
        secrets = sdk.all_embed_secrets()
        if not secrets:
            print("  No embed secrets found")
            return
        for s in secrets:
            safe_delete(audit, 5, sdk.delete_embed_secret, "embed_secret", s.id, dry_run)
    except AttributeError:
        # SDK doesn't expose all_embed_secrets — try resetting via setting
        try:
            setting = sdk.embed_config()
            print(f"  Embed config accessible (enabled={getattr(setting, 'embed_enabled', '?')}), but no delete API in this SDK version — skip")
        except Exception:
            print("  Embed secret endpoints not available in this SDK version — skip")


def step_06_delete_users(sdk, audit, dry_run, protected_user_id: int):
    # TODO: Currently protects all admins. Revisit if more granular control is needed
    # (e.g. protect by email list, or only protect the API caller).
    step_header(6, "Delete all users (protecting all admins)")
    try:
        users = sdk.all_users()
    except Exception as e:
        print(f"  Could not list users: {e}")
        return

    # Build set of admin user IDs by checking role assignments
    admin_ids = {protected_user_id}
    for user in users:
        try:
            roles = sdk.user_roles(user.id)
            if any(r.name == "Admin" for r in roles):
                admin_ids.add(user.id)
        except Exception:
            pass
    print(f"  Found {len(admin_ids)} admin user(s) to protect: {sorted(admin_ids)}")

    for user in users:
        if user.id in admin_ids:
            audit.record(6, "DELETE", "user", user.id, "skipped", "admin user protected")
            continue
        if not dry_run:
            # Delete credentials first
            try:
                sdk.delete_user_credentials_email(user.id)
            except Exception:
                pass
            try:
                sdk.delete_user_credentials_api3(user.id, user.credentials_api3[0].id if user.credentials_api3 else "")
            except Exception:
                pass
            try:
                sdk.delete_user_sessions(user.id)
            except Exception:
                pass
        safe_delete(audit, 6, sdk.delete_user, "user", user.id, dry_run)


def step_07_delete_dashboards(sdk, audit, dry_run):
    step_header(7, "Hard-delete all dashboards (including soft-deleted)")
    deleted_ids: set = set()
    for deleted_param in [False, True]:
        try:
            dashboards = sdk.search_dashboards(deleted=deleted_param, limit=5000)
        except Exception as e:
            print(f"  Could not list dashboards (deleted={deleted_param}): {e}")
            continue
        for d in dashboards:
            if d.id in deleted_ids:
                continue
            deleted_ids.add(d.id)
            safe_delete(audit, 7, sdk.delete_dashboard, "dashboard", d.id, dry_run)


def step_08_delete_looks(sdk, audit, dry_run):
    step_header(8, "Hard-delete all Looks (including soft-deleted)")
    deleted_ids: set = set()
    for deleted_param in [False, True]:
        try:
            looks = sdk.search_looks(deleted=deleted_param, limit=5000)
        except Exception as e:
            print(f"  Could not list looks (deleted={deleted_param}): {e}")
            continue
        for lk in looks:
            if lk.id in deleted_ids:
                continue
            deleted_ids.add(lk.id)
            safe_delete(audit, 8, sdk.delete_look, "look", lk.id, dry_run)


def step_09_delete_folders(sdk, audit, dry_run):
    step_header(9, "Delete all non-system folders (cascading)")
    # System folders: id "1" (Shared), "lookml" (LookML dashboards), personal home folders
    SYSTEM_FOLDER_IDS = {"1", "lookml"}
    try:
        folders = sdk.all_folders()
    except Exception as e:
        print(f"  Could not list folders: {e}")
        return
    # Sort leaf-first so cascading works; child folders before parents
    # Heuristic: longer parent_id chains → delete children first
    # Simple approach: retry failures once (parent may need child gone first)
    deletable = [f for f in folders if str(f.id) not in SYSTEM_FOLDER_IDS and not getattr(f, "is_personal", False) and not getattr(f, "is_personal_descendant", False)]

    failed = []
    for folder in deletable:
        ok = safe_delete(audit, 9, sdk.delete_folder, "folder", folder.id, dry_run)
        if not ok:
            failed.append(folder)
    # Retry once for parent ordering issues
    for folder in failed:
        safe_delete(audit, 9, sdk.delete_folder, "folder", folder.id, dry_run)


def step_10_delete_boards(sdk, audit, dry_run):
    step_header(10, "Delete all boards (homepages)")
    try:
        boards = sdk.all_boards()
    except Exception as e:
        print(f"  Could not list boards: {e}")
        return
    for b in boards:
        safe_delete(audit, 10, sdk.delete_board, "board", b.id, dry_run)


def step_11_delete_connections(sdk, audit, dry_run):
    step_header(11, "Delete all database connections")
    try:
        conns = sdk.all_connections()
    except Exception as e:
        print(f"  Could not list connections: {e}")
        return
    for c in conns:
        safe_delete(audit, 11, sdk.delete_connection, "connection", c.name, dry_run)


def step_12_delete_ssh(sdk, audit, dry_run):
    step_header(12, "Delete all SSH tunnels and servers")
    try:
        tunnels = sdk.all_ssh_tunnels()
        for t in tunnels:
            safe_delete(audit, 12, sdk.delete_ssh_tunnel, "ssh_tunnel", t.id, dry_run)
    except Exception as e:
        print(f"  SSH tunnels not accessible or none present: {e}")
    try:
        servers = sdk.all_ssh_servers()
        for s in servers:
            safe_delete(audit, 12, sdk.delete_ssh_server, "ssh_server", s.id, dry_run)
    except Exception as e:
        print(f"  SSH servers not accessible or none present: {e}")


def step_13_delete_themes(sdk, audit, dry_run):
    step_header(13, "Delete all custom themes (skip built-in default)")
    try:
        themes = sdk.all_themes()
    except Exception as e:
        print(f"  Could not list themes: {e}")
        return
    for t in themes:
        if t.name and t.name.lower() in ("looker", "default"):
            audit.record(13, "DELETE", "theme", t.id, "skipped", f"built-in theme '{t.name}'")
            continue
        safe_delete(audit, 13, sdk.delete_theme, "theme", t.id, dry_run)


def step_14_delete_color_collections(sdk, audit, dry_run):
    step_header(14, "Delete all custom color collections (skip built-in)")
    try:
        collections = sdk.all_color_collections()
    except Exception as e:
        print(f"  Could not list color collections: {e}")
        return
    for c in collections:
        if getattr(c, "is_default", False) or getattr(c, "type", "") == "system":
            audit.record(14, "DELETE", "color_collection", c.id, "skipped", "built-in collection")
            continue
        safe_delete(audit, 14, sdk.delete_color_collection, "color_collection", c.id, dry_run)


def step_15_delete_user_attributes(sdk, audit, dry_run):
    step_header(15, "Delete all user attributes")
    try:
        attrs = sdk.all_user_attributes()
    except Exception as e:
        print(f"  Could not list user attributes: {e}")
        return
    for a in attrs:
        if getattr(a, "is_system", False):
            audit.record(15, "DELETE", "user_attribute", a.id, "skipped", "system attribute")
            continue
        safe_delete(audit, 15, sdk.delete_user_attribute, "user_attribute", a.id, dry_run)


def step_16_delete_groups(sdk, audit, dry_run):
    step_header(16, "Delete all custom groups (skip built-in 'All Users')")
    try:
        groups = sdk.all_groups()
    except Exception as e:
        print(f"  Could not list groups: {e}")
        return
    for g in groups:
        if g.name == "All Users":
            audit.record(16, "DELETE", "group", g.id, "skipped", "built-in 'All Users' group")
            continue
        safe_delete(audit, 16, sdk.delete_group, "group", g.id, dry_run)


def step_17_delete_roles_permissions_models(sdk, audit, dry_run):
    step_header(17, "Delete custom roles, permission sets, model sets (skip built-in)")
    # Model sets
    try:
        model_sets = sdk.all_model_sets()
        for ms in model_sets:
            if getattr(ms, "built_in", False):
                audit.record(17, "DELETE", "model_set", ms.id, "skipped", "built-in")
                continue
            safe_delete(audit, 17, sdk.delete_model_set, "model_set", ms.id, dry_run)
    except Exception as e:
        print(f"  Could not process model sets: {e}")

    # Permission sets
    try:
        perm_sets = sdk.all_permission_sets()
        for ps in perm_sets:
            if getattr(ps, "built_in", False):
                audit.record(17, "DELETE", "permission_set", ps.id, "skipped", "built-in")
                continue
            safe_delete(audit, 17, sdk.delete_permission_set, "permission_set", ps.id, dry_run)
    except Exception as e:
        print(f"  Could not process permission sets: {e}")

    # Roles
    try:
        roles = sdk.all_roles()
        for r in roles:
            if getattr(r, "built_in", False):
                audit.record(17, "DELETE", "role", r.id, "skipped", "built-in")
                continue
            safe_delete(audit, 17, sdk.delete_role, "role", r.id, dry_run)
    except Exception as e:
        print(f"  Could not process roles: {e}")


def step_18_delete_integration_hubs(sdk, audit, dry_run):
    step_header(18, "Delete all integration hubs")
    try:
        hubs = sdk.all_integration_hubs()
    except Exception as e:
        print(f"  Could not list integration hubs: {e}")
        return
    for h in hubs:
        safe_delete(audit, 18, sdk.delete_integration_hub, "integration_hub", h.id, dry_run)


def step_19_neutralize_auth_configs(sdk, audit, dry_run):
    step_header(19, "Neutralize OIDC / SAML / LDAP configs (overwrite with dummy values)")
    dummy_url = "https://disabled.invalid"
    dummy_str = "WIPED"

    # OIDC
    try:
        oidc = sdk.oidc_config()
        if oidc and getattr(oidc, "enabled", False):
            body = models.WriteOIDCConfig(
                enabled=False,
                issuer=dummy_url,
                client_id=dummy_str,
                client_secret=dummy_str,
            )
            safe_patch(audit, 19, lambda _id, b: sdk.update_oidc_config(b), "oidc_config", "oidc", dry_run, body)
        else:
            print("  OIDC not enabled, skipping")
    except Exception as e:
        print(f"  OIDC config not accessible: {e}")

    # SAML
    try:
        saml = sdk.saml_config()
        if saml and getattr(saml, "enabled", False):
            body = models.WriteSamlConfig(
                enabled=False,
                idp_issuer=dummy_url,
                idp_url=dummy_url,
                idp_cert=dummy_str,
            )
            safe_patch(audit, 19, lambda _id, b: sdk.update_saml_config(b), "saml_config", "saml", dry_run, body)
        else:
            print("  SAML not enabled, skipping")
    except Exception as e:
        print(f"  SAML config not accessible: {e}")

    # LDAP
    try:
        ldap = sdk.ldap_config()
        if ldap and getattr(ldap, "enabled", False):
            body = models.WriteLDAPConfig(
                enabled=False,
                url=dummy_url,
                bind_dn=dummy_str,
                bind_password=dummy_str,
            )
            safe_patch(audit, 19, lambda _id, b: sdk.update_ldap_config(b), "ldap_config", "ldap", dry_run, body)
        else:
            print("  LDAP not enabled, skipping")
    except Exception as e:
        print(f"  LDAP config not accessible: {e}")


def step_20_flush_caches(sdk, audit, dry_run):
    step_header(20, "Flush all query result caches via datagroups")
    now_ts = int(time.time())
    try:
        datagroups = sdk.all_datagroups()
    except Exception as e:
        print(f"  Could not list datagroups: {e}")
        return
    if not datagroups:
        print("  No datagroups found")
        return
    for dg in datagroups:
        body = models.WriteDatagroup(stale_before=now_ts)
        safe_patch(audit, 20, lambda _id, b: sdk.update_datagroup(_id, b), "datagroup", dg.id, dry_run, body)


def step_21_delete_lookml_models(sdk, audit, dry_run):
    step_header(21, "Delete all LookML models")
    try:
        models_list = sdk.all_lookml_models()
    except Exception as e:
        print(f"  Could not list LookML models: {e}")
        return
    for m in models_list:
        safe_delete(audit, 21, sdk.delete_lookml_model, "lookml_model", m.name, dry_run)


def step_22_delete_git_branches(sdk, audit, dry_run):
    step_header(22, "Delete dev Git branches (SKIPPED)")
    print("  Step removed: Git branches are no longer deleted via this script.")


def step_23_finalize_audit(audit: AuditLog):
    step_header(23, "Finalize audit log")
    audit.save()
    summary = audit.summary()
    print(f"\n  Summary: {summary}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_sdk(base_url: str, client_id: str, client_secret: str):
    """Build SDK from explicit credentials, using looker.ini for SSL/timeout settings."""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    os.environ["LOOKERSDK_BASE_URL"] = base_url
    os.environ["LOOKERSDK_CLIENT_ID"] = client_id
    os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret
    ini_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "looker.ini")
    return looker_sdk.init40(config_file=ini_path, section="Looker")


def main():
    parser = argparse.ArgumentParser(
        description="Looker instance wipe — Phase 1 API-driven teardown"
    )
    parser.add_argument("--base-url", help="Looker base URL (e.g. https://myco.looker.com)")
    parser.add_argument("--client-id", help="API3 client ID")
    parser.add_argument("--client-secret", help="API3 client secret")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without making any changes",
    )
    parser.add_argument(
        "--steps",
        help="Comma-separated step numbers to run (default: all). E.g. --steps 1,2,6",
    )
    parser.add_argument(
        "--audit-log",
        help="Path for audit log JSON (default: wipe_audit_<timestamp>.json)",
    )
    args = parser.parse_args()

    # --- Credentials: args > env vars > looker.ini (SDK default) ---
    base_url = args.base_url or os.environ.get("LOOKER_BASE_URL")
    client_id = args.client_id or os.environ.get("LOOKER_CLIENT_ID")
    client_secret = args.client_secret or os.environ.get("LOOKER_CLIENT_SECRET")

    if base_url and client_id and client_secret:
        sdk = build_sdk(base_url, client_id, client_secret)
    else:
        # Fall back to looker.ini / environment variables via SDK default
        print("No explicit credentials provided — falling back to looker.ini / LOOKERSDK_* env vars")
        sdk = looker_sdk.init40()

    # --- Audit log ---
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    audit_path = args.audit_log or f"wipe_audit_{ts}.json"
    audit = AuditLog(audit_path)
    audit.dry_run = args.dry_run

    # --- Steps to run ---
    all_steps = set(range(1, 24))
    if args.steps:
        try:
            requested = {int(s.strip()) for s in args.steps.split(",")}
        except ValueError:
            print("ERROR: --steps must be comma-separated integers")
            sys.exit(1)
        run_steps = requested & all_steps
    else:
        run_steps = all_steps

    # --- Safety check: confirm before real run ---
    if not args.dry_run:
        try:
            me = sdk.me()
            protected_id = me.id
            display_name = me.display_name or me.email or str(me.id)
        except Exception as e:
            print(f"ERROR: Cannot authenticate with Looker: {e}")
            sys.exit(1)

        instance_url = base_url or os.environ.get("LOOKER_BASE_URL", "(from looker.ini)")
        print(f"\n{'='*60}")
        print(f"  LOOKER INSTANCE WIPE — PHASE 1 TEARDOWN")
        print(f"{'='*60}")
        print(f"  Instance : {instance_url}")
        print(f"  Admin    : {display_name} (id={protected_id})")
        print(f"  Steps    : {sorted(run_steps)}")
        print(f"  Audit log: {audit_path}")
        print(f"{'='*60}")
        print(f"\n  *** THIS WILL PERMANENTLY DELETE DATA. THIS CANNOT BE UNDONE. ***\n")
        confirm = input("  Type 'wipe' to confirm, or anything else to cancel: ").strip()
        if confirm != "wipe":
            print("Aborted.")
            sys.exit(0)
    else:
        try:
            me = sdk.me()
            protected_id = me.id
        except Exception as e:
            print(f"ERROR: Cannot authenticate with Looker: {e}")
            sys.exit(1)
        instance_url = base_url or os.environ.get("LOOKER_BASE_URL", "(from looker.ini)")
        print(f"\n{'='*60}")
        print(f"  LOOKER INSTANCE WIPE — DRY RUN (no changes will be made)")
        print(f"{'='*60}")
        print(f"  Instance : {instance_url}")
        print(f"  Admin    : {me.display_name} (id={protected_id})")
        print(f"  Steps    : {sorted(run_steps)}")
        print(f"{'='*60}")

    step_map = {
        1:  lambda: step_01_kill_running_queries(sdk, audit, args.dry_run),
        2:  lambda: step_02_delete_scheduled_plans(sdk, audit, args.dry_run),
        3:  lambda: step_03_delete_alerts(sdk, audit, args.dry_run),
        4:  lambda: step_04_delete_oauth_apps(sdk, audit, args.dry_run),
        5:  lambda: step_05_delete_embed_secrets(sdk, audit, args.dry_run),
        6:  lambda: step_06_delete_users(sdk, audit, args.dry_run, protected_id),  # protects all admins
        7:  lambda: step_07_delete_dashboards(sdk, audit, args.dry_run),
        8:  lambda: step_08_delete_looks(sdk, audit, args.dry_run),
        9:  lambda: step_09_delete_folders(sdk, audit, args.dry_run),
        10: lambda: step_10_delete_boards(sdk, audit, args.dry_run),
        11: lambda: step_11_delete_connections(sdk, audit, args.dry_run),
        12: lambda: step_12_delete_ssh(sdk, audit, args.dry_run),
        13: lambda: step_13_delete_themes(sdk, audit, args.dry_run),
        14: lambda: step_14_delete_color_collections(sdk, audit, args.dry_run),
        15: lambda: step_15_delete_user_attributes(sdk, audit, args.dry_run),
        16: lambda: step_16_delete_groups(sdk, audit, args.dry_run),
        17: lambda: step_17_delete_roles_permissions_models(sdk, audit, args.dry_run),
        18: lambda: step_18_delete_integration_hubs(sdk, audit, args.dry_run),
        19: lambda: step_19_neutralize_auth_configs(sdk, audit, args.dry_run),
        20: lambda: step_20_flush_caches(sdk, audit, args.dry_run),
        21: lambda: step_21_delete_lookml_models(sdk, audit, args.dry_run),
        22: lambda: step_22_delete_git_branches(sdk, audit, args.dry_run),
        23: lambda: step_23_finalize_audit(audit),
    }

    start = time.time()
    for step_n in sorted(run_steps):
        if step_n == 23:
            continue  # always run last
        try:
            step_map[step_n]()
        except Exception as e:
            print(f"  UNHANDLED ERROR in step {step_n}: {e}")
            traceback.print_exc()

    step_23_finalize_audit(audit)
    elapsed = time.time() - start
    print(f"\nCompleted in {elapsed:.1f}s")

    if args.dry_run:
        print("\nThis was a DRY RUN. Re-run without --dry-run to execute.")
    else:
        print("\nPhase 1 complete.")
        print("Next: Phase 2 (manual UI) — delete LookML projects, verify Trash, verify System Activity.")
        print("Next: Phase 3 (support ticket) — request deletion attestation, Git branch cleanup, backup purge confirmation.")


if __name__ == "__main__":
    main()
