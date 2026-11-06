"""Create and wire up the StrideForge x Greenstar clean room (bundles cannot manage clean rooms, so this is scripted).

Steps (each is idempotent and can be run on its own with --step):
  create     StrideForge (target profile) creates the clean room with Greenstar as collaborator, waits for ACTIVE,
             and creates the clean room output catalog.
  tables     Each party shares its own table: StrideForge retail_cleanroom.cr_match_keys, Greenstar
             consumer.person_attributes.
  notebooks  Greenstar (provider profile) uploads the clean room notebooks with StrideForge as the only runner.
  approve    StrideForge reviews and approves Greenstar's notebooks (the uploader's approval is implicit).
  status     Prints the room, its assets and review state.

Prerequisites: CREATE CLEAN ROOM on both metastores (needed to create the room and to get a sharing identifier),
and retail_cleanroom.cr_match_keys must exist (run job strideforge_greenstar_enrichment tasks ddl + refresh_match_keys).

Usage (from the repo root):
  python3 src/cleanroom/scripts/setup_clean_room.py --step all
"""
import argparse
import base64
import os
import time

from _dbsql import cli

HERE = os.path.dirname(os.path.abspath(__file__))
NOTEBOOKS = {
    "greenstar_validate_access": "Greenstar access check: counts rows on both sides; writes only aggregates.",
    "greenstar_enrich_customers": "Greenstar Consumer Data enrichment: matches on hashed email and returns summaries "
                                  "(generation, income band, life stage, ...) plus phone where StrideForge has none.",
}


def args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", default="all", choices=["all", "create", "tables", "notebooks", "approve", "status"])
    ap.add_argument("--room", default="strideforge_greenstar_enrichment")
    ap.add_argument("--target-profile", default="fevm-vijay")
    ap.add_argument("--target-catalog", default="vijay_catalog")
    ap.add_argument("--cleanroom-schema", default="retail_cleanroom")
    ap.add_argument("--provider-profile", default="partner-demo-catalog")
    ap.add_argument("--provider-catalog", default="greenstar_demo")
    ap.add_argument("--provider-workspace-dir", default="/Shared/greenstar_clean_room")
    return ap.parse_args()


def sharing_identifier(profile):
    """The Clean Room sharing identifier = global metastore id + workspace id + the user's email."""
    ms = cli(profile, "metastores", "summary")
    cur = cli(profile, "metastores", "current")
    me = cli(profile, "current-user", "me")
    return {"global_metastore_id": ms["global_metastore_id"], "workspace_id": cur["workspace_id"],
            "email": me["userName"], "cloud": ms["cloud"], "region": ms["region"]}


def room(a, profile):
    try:
        return cli(profile, "clean-rooms", "get", a.room)
    except RuntimeError as e:
        if "does not exist" in str(e).lower() or "not found" in str(e).lower():
            return None
        raise


def step_create(a):
    r = room(a, a.target_profile)
    if not r:
        sf, gs = sharing_identifier(a.target_profile), sharing_identifier(a.provider_profile)
        print(f"Creating {a.room}: strideforge={sf['global_metastore_id']} greenstar={gs['global_metastore_id']} ({gs['email']})")
        cli(a.target_profile, "clean-rooms", "create", "--no-wait", body={
            "name": a.room,
            "comment": "StrideForge x Greenstar Consumer Data: privacy-safe customer enrichment (demo, synthetic data).",
            "remote_detailed_info": {
                "cloud_vendor": sf["cloud"], "region": sf["region"],
                # No internet egress from clean room compute: notebooks can't send data anywhere.
                "egress_network_policy": {"internet_access": {"restriction_mode": "RESTRICTED_ACCESS"}},
                "collaborators": [
                    {"collaborator_alias": "strideforge", "global_metastore_id": sf["global_metastore_id"]},
                    {"collaborator_alias": "greenstar", "global_metastore_id": gs["global_metastore_id"],
                     "invite_recipient_email": gs["email"], "invite_recipient_workspace_id": gs["workspace_id"]},
                ],
            },
        })
    for _ in range(80):
        r = room(a, a.target_profile)
        if r and r.get("status") == "ACTIVE":
            break
        print(f"  status: {r and r.get('status')}"); time.sleep(15)
    else:
        raise SystemExit("Clean room did not become ACTIVE in 20 minutes")
    print(f"{a.room} is ACTIVE (central id {r['remote_detailed_info'].get('central_clean_room_id')})")
    if r.get("output_catalog", {}).get("status") != "CREATED":
        cli(a.target_profile, "clean-rooms", "create-output-catalog", a.room,
            body={"catalog_name": f"{a.room}_output"})
        print(f"Created output catalog {a.room}_output")
    # The provider side shows up a little later.
    for _ in range(40):
        if room(a, a.provider_profile):
            print("Greenstar sees the clean room"); return
        time.sleep(15)
    raise SystemExit("Clean room has not appeared on the provider metastore yet; re-run --step tables later")


def assets(a, profile):
    return cli(profile, "clean-room-assets", "list", a.room) or []


def add_table(a, profile, alias, local_name):
    shared = f"{alias}.{'.'.join(local_name.split('.')[1:])}"
    if any(x["name"] == shared for x in assets(a, profile)):
        print(f"  {shared} already shared"); return
    cli(profile, "clean-room-assets", "create", a.room, body={
        "name": shared, "asset_type": "TABLE", "table_local_details": {"local_name": local_name}, "table": {}})
    print(f"  shared {local_name} as {shared}")


def step_tables(a):
    add_table(a, a.target_profile, "strideforge", f"{a.target_catalog}.{a.cleanroom_schema}.cr_match_keys")
    add_table(a, a.provider_profile, "greenstar", f"{a.provider_catalog}.consumer.person_attributes")


def step_notebooks(a):
    cli(a.provider_profile, "workspace", "mkdirs", a.provider_workspace_dir)
    existing = {x["name"]: x for x in assets(a, a.provider_profile)}
    for nb, desc in NOTEBOOKS.items():
        src = os.path.join(HERE, "..", "clean_room_notebooks", f"{nb}.py")
        path = f"{a.provider_workspace_dir}/{nb}"
        with open(src, "rb") as f:
            content = base64.b64encode(f.read()).decode()
        cli(a.provider_profile, "workspace", "import", body={
            "path": path, "format": "SOURCE", "language": "PYTHON", "content": content, "overwrite": True})
        html = cli(a.provider_profile, "workspace", "export", path, "--format", "HTML")["content"]
        body = {"notebook": {"notebook_content": html, "runner_collaborator_aliases": ["strideforge"],
                             "description": desc, "environment_version": "4"}}
        if nb in existing:
            cli(a.provider_profile, "clean-room-assets", "update", a.room, "NOTEBOOK_FILE", nb,
                body={**body, "name": nb, "asset_type": "NOTEBOOK_FILE"})
            print(f"  updated notebook {nb} (new version needs approval again)")
        else:
            cli(a.provider_profile, "clean-room-assets", "create", a.room, body={**body, "name": nb, "asset_type": "NOTEBOOK_FILE"})
            print(f"  uploaded notebook {nb}")


def step_approve(a):
    for x in assets(a, a.target_profile):
        if x["asset_type"] != "NOTEBOOK_FILE" or x["name"] not in NOTEBOOKS:
            continue
        nb = x.get("notebook", {})
        if nb.get("review_state") == "APPROVED":
            print(f"  {x['name']}: already approved"); continue
        cli(a.target_profile, "clean-room-assets", "create-clean-room-asset-review", a.room, "NOTEBOOK_FILE", x["name"],
            body={"notebook_review": {"review_state": "APPROVED", "etag": nb.get("etag"),
                                      "comment": "Reviewed by StrideForge: returns summaries only, plus phone where we have none."}})
        print(f"  approved {x['name']}")


def step_status(a):
    r = room(a, a.target_profile)
    print(f"{a.room}: {r and r.get('status')}, output catalog: {r and r.get('output_catalog')}")
    for x in assets(a, a.target_profile):
        print(f"  {x['asset_type']:14} {x['name']:55} owner={x.get('owner_collaborator_alias')} "
              f"{x.get('status')} {x.get('notebook', {}).get('review_state', '')}")


def main():
    a = args()
    steps = ["create", "tables", "notebooks", "approve", "status"] if a.step == "all" else [a.step]
    for s in steps:
        print(f"== {s}")
        globals()[f"step_{s}"](a)


if __name__ == "__main__":
    main()
