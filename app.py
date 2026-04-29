"""
Bound Policies (BD view) — standalone FastAPI app.

Pulls open FLT Jira tickets (via Atlassian REST) and Salesforce opportunity data
(via simple-salesforce), joins them, and serves both:

    GET /             → the React frontend (index.html)
    GET /api/data     → the joined JSON payload the frontend renders

Run locally:
    pip install -r requirements.txt
    cp .env.example .env  # fill in credentials
    python app.py

Then open http://localhost:8000.
"""

from __future__ import annotations

import os
import json
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from simple_salesforce import Salesforce, SalesforceError

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# BD Portal — separate Atlassian customer account used for posting BD-relayed
# comments. Comments authored as this account show up clearly in Jira as
# distinct from internal Ops activity.
BD_PORTAL_EMAIL      = os.getenv("BD_PORTAL_EMAIL")
BD_PORTAL_TOKEN      = os.getenv("BD_PORTAL_TOKEN")
BD_PORTAL_ACCOUNT_ID = os.getenv("BD_PORTAL_ACCOUNT_ID")  # set once after customer creation

# ─── CONFIG ────────────────────────────────────────────────────────────────────

JIRA_BASE_URL = os.getenv("JIRA_BASE_URL", "https://nirvana-team.atlassian.net")
JIRA_EMAIL    = os.getenv("JIRA_EMAIL")
JIRA_TOKEN    = os.getenv("JIRA_TOKEN")

SFDC_USERNAME        = os.getenv("SFDC_USERNAME")
SFDC_PASSWORD        = os.getenv("SFDC_PASSWORD")
SFDC_SECURITY_TOKEN  = os.getenv("SFDC_SECURITY_TOKEN")  # only required if Connected App requires IP-based security
SFDC_CONSUMER_KEY    = os.getenv("SFDC_CONSUMER_KEY")    # Connected App OAuth2
SFDC_CONSUMER_SECRET = os.getenv("SFDC_CONSUMER_SECRET")
SFDC_DOMAIN          = os.getenv("SFDC_DOMAIN", "login")  # "test" for sandbox

# Verified via Atlassian API on 2026-04-29 — these IDs are stable for FLT.
F_NOTICE_TYPE        = "customfield_10145"
F_DOT_NUMBER         = "customfield_10184"   # DOT — kept visible alongside opportunity
F_OPPORTUNITY_NAME   = "customfield_11202"   # primary account grouping key
F_AL_POLICY          = "customfield_10130"   # Auto Liability Policy Number
F_GL_POLICY          = "customfield_11304"   # General Liability Policy Number
F_MTC_POLICY         = "customfield_11305"   # Motor Truck Cargo Policy Number
F_EFFECTIVE          = "customfield_10157"
F_WORDING_FOR_NOTICE = "customfield_10150"   # Official notice wording sent to the agency

JIRA_FIELDS = [
    "summary", "status", "issuetype", "assignee",
    "created", "updated", "priority", "duedate",
    F_NOTICE_TYPE, F_DOT_NUMBER, F_OPPORTUNITY_NAME,
    F_AL_POLICY, F_GL_POLICY, F_MTC_POLICY, F_EFFECTIVE,
]

OPEN_JQL = "project = FLT AND statusCategory != Done ORDER BY created DESC"

# Salesforce Opportunity fields. Adjust to match your org's custom field API names
# if they differ — these are reasonable defaults for a typical commercial-trucking
# opportunity object.
SFDC_OPPORTUNITY_FIELDS = [
    "Id", "Name", "StageName", "Amount", "CloseDate",
    "DOT_Number__c", "Policy_Number__c", "Effective_Date__c",
    "Account.Name", "Owner.Name", "Owner.Email",
]
SFDC_DOT_FIELD     = "DOT_Number__c"
SFDC_POLICY_FIELD  = "Policy_Number__c"


# ─── JIRA ──────────────────────────────────────────────────────────────────────

def jira_auth_header() -> dict:
    if not (JIRA_EMAIL and JIRA_TOKEN):
        raise HTTPException(500, "JIRA_EMAIL and JIRA_TOKEN must be set in .env")
    raw = f"{JIRA_EMAIL}:{JIRA_TOKEN}".encode()
    return {"Authorization": f"Basic {base64.b64encode(raw).decode()}",
            "Accept": "application/json"}


def fetch_jira_page(next_page_token=None, page_size=100):
    """Calls the new /rest/api/3/search/jql endpoint (cursor-paginated)."""
    params = {
        "jql": OPEN_JQL,
        "fields": ",".join(JIRA_FIELDS),
        "maxResults": page_size,
    }
    if next_page_token:
        params["nextPageToken"] = next_page_token
    resp = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/search/jql",
        headers=jira_auth_header(),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_all_open_flt():
    issues = []
    next_token = None
    for _ in range(30):  # 3000-ticket safety ceiling
        page = fetch_jira_page(next_token)
        issues.extend(page.get("issues", []))
        next_token = page.get("nextPageToken")
        if not next_token or page.get("isLast"):
            break
    return issues


# ─── CLASSIFICATION ────────────────────────────────────────────────────────────

LANE_CANCELLATION = "cancellations"
LANE_NONRENEWAL   = "nonrenewals"
LANE_CONDITIONAL  = "conditional"
LANE_OTHER        = "other"


def classify(notice_type, issue_type, summary=""):
    """
    Cancellation and conditional/nonrenewal tickets are identified primarily by
    issue type. The Notice Type custom field disambiguates conditional vs
    nonrenewal but is sometimes unset; in that case fall back to the title.
    """
    nt_l    = (notice_type or "").lower()
    title_l = (summary or "").lower()

    if issue_type == "Notice Issuance -Cancelation":
        return LANE_CANCELLATION

    if issue_type == "Notice Issuance-Conditional or Nonrenewal":
        # Substring match — the option value is sometimes "Conditional",
        # sometimes "Conditional Renewal", sometimes "Conditional Notice".
        if "conditional" in nt_l:
            return LANE_CONDITIONAL
        if "nonrenewal" in nt_l or "non-renewal" in nt_l or "non renewal" in nt_l:
            return LANE_NONRENEWAL
        # Notice Type unset — try the title.
        if "conditional" in title_l:
            return LANE_CONDITIONAL
        if "nonrenewal" in title_l or "non-renewal" in title_l or "non renewal" in title_l:
            return LANE_NONRENEWAL
        return LANE_NONRENEWAL  # last-resort default

    if notice_type:
        if nt_l.startswith("cancellation -"):
            return LANE_CANCELLATION
        if "conditional" in nt_l:
            return LANE_CONDITIONAL
        if "nonrenewal" in nt_l:
            return LANE_NONRENEWAL

    return LANE_OTHER


def _normalize_status(s):
    """Normalize for lookup: lowercase, collapse whitespace around dashes."""
    if not s:
        return ""
    return " ".join(s.lower().replace("-", " ").split())


# Plain-English meanings of internal statuses. Mirrors the frontend mapping but
# is BD-facing — no contractor names, no internal jargon. Fed into the LLM
# prompt as ground truth so the summary aligns with what the status actually
# means in BD-speak (rather than the LLM guessing from comments).
STATUS_DESCRIPTIONS = {
    "to do":                       "In our queue — work has not yet started",
    "todo":                        "In our queue — work has not yet started",
    "in progress":                 "Our team is actively working on this",
    "in progress flatworld":       "Our team is currently drafting the notice",
    "on hold":                     "Sent out — awaiting an agent or stakeholder response",
    "on hold flatworld":           "Notice has been sent to the agent — awaiting cancel date or rescind request",
    "cancel endorsement needed":   "Cancel date reached — cancellation endorsement is needed",
    "rescission needed":           "Our team is rescinding the cancellation notice — the cancellation will NOT take effect; the policy stays in force",
    "recission needed":            "Our team is rescinding the cancellation notice — the cancellation will NOT take effect; the policy stays in force",
    "renewal review pending":      "In our queue — work not yet started",
    "policy issuance in progress": "Policy is being issued",
    "open":                        "Open — needs review",
    "reopened":                    "Reopened — back in our queue",
}


def status_meaning(status):
    return STATUS_DESCRIPTIONS.get(_normalize_status(status), "")


# Workflow-stage urgency. Lower = more urgent. Surfaces "Cancel Endorsement
# Needed" / "Rescission Needed" before earlier-stage work. Keys are normalized.
_STATUS_URGENCY = {
    "rescission needed":           1,
    "recission needed":            1,  # Jira has the typo
    "cancel endorsement needed":   2,
    "on hold flatworld":           3,
    "on hold":                     3,
    "to do":                       4,
    "todo":                        4,
    "renewal review pending":      5,
    "in progress":                 6,
    "in progress flatworld":       7,
    "policy issuance in progress": 6,
}


def status_urgency(status):
    return _STATUS_URGENCY.get(_normalize_status(status), 999)


def status_tone(status):
    """UI tone bucket for color coding the status pill."""
    n = _normalize_status(status)
    if not n:
        return "neutral"
    if n in ("rescission needed", "recission needed", "cancel endorsement needed"):
        return "urgent"
    if "on hold" in n or n in ("to do", "todo", "renewal review pending"):
        return "waiting"
    if "in progress" in n:
        return "active"
    return "neutral"


# Backwards-compat alias for any older code referencing the dict directly.
STATUS_URGENCY = _STATUS_URGENCY


# ─── NORMALIZATION ─────────────────────────────────────────────────────────────

def days_since(iso_str):
    if not iso_str:
        return None
    try:
        s = iso_str.replace("Z", "+00:00")
        if len(s) >= 5 and s[-5] in "+-" and s[-3] != ":":
            s = s[:-2] + ":" + s[-2:]
        dt = datetime.fromisoformat(s)
        return (datetime.now(timezone.utc) - dt).days
    except Exception:
        return None


def get_option_value(field):
    if field is None:
        return None
    if isinstance(field, dict):
        return field.get("value") or field.get("name")
    if isinstance(field, list) and field:
        return get_option_value(field[0])
    return str(field)


def _str_field(value):
    return value.strip() if isinstance(value, str) and value.strip() else None


def normalize_ticket(raw):
    fields = raw.get("fields", {})
    notice_type = get_option_value(fields.get(F_NOTICE_TYPE))
    issue_type  = (fields.get("issuetype") or {}).get("name")
    status      = (fields.get("status") or {}).get("name")

    return {
        "key":              raw.get("key"),
        "summary":          fields.get("summary"),
        "issue_type":       issue_type,
        "notice_type":      notice_type,
        "status":           status,
        "status_tone":      status_tone(status),
        "assignee":         (fields.get("assignee") or {}).get("displayName"),
        "priority":         (fields.get("priority") or {}).get("name"),
        "opportunity_name": _str_field(fields.get(F_OPPORTUNITY_NAME)),
        "dot":              _str_field(fields.get(F_DOT_NUMBER)),
        "al_policy":        _str_field(fields.get(F_AL_POLICY)),
        "gl_policy":        _str_field(fields.get(F_GL_POLICY)),
        "mtc_policy":       _str_field(fields.get(F_MTC_POLICY)),
        "created":          fields.get("created"),
        "updated":          fields.get("updated"),
        "due_date":         fields.get("duedate"),
        "effective":        fields.get(F_EFFECTIVE),
        "age_days":         days_since(fields.get("created")),
        "lane":             classify(notice_type, issue_type, fields.get("summary") or ""),
        "url":              f"{JIRA_BASE_URL}/browse/{raw.get('key')}",
    }


# ─── SALESFORCE ────────────────────────────────────────────────────────────────

def sfdc_client() -> Salesforce:
    """
    Authenticates to Salesforce. Two paths supported:

    1. OAuth2 password flow — uses SFDC_CONSUMER_KEY + SFDC_CONSUMER_SECRET (a
       Connected App). Doesn't always need a security token; depends on the
       Connected App's IP restrictions.
    2. Legacy username/password + security token flow.
    """
    if SFDC_CONSUMER_KEY and SFDC_CONSUMER_SECRET:
        host = "test.salesforce.com" if SFDC_DOMAIN == "test" else "login.salesforce.com"
        password = (SFDC_PASSWORD or "") + (SFDC_SECURITY_TOKEN or "")
        resp = requests.post(
            f"https://{host}/services/oauth2/token",
            data={
                "grant_type":    "password",
                "client_id":     SFDC_CONSUMER_KEY,
                "client_secret": SFDC_CONSUMER_SECRET,
                "username":      SFDC_USERNAME,
                "password":      password,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"SFDC OAuth failed ({resp.status_code}): {resp.text[:300]}")
        token = resp.json()
        return Salesforce(instance_url=token["instance_url"], session_id=token["access_token"])

    if not (SFDC_USERNAME and SFDC_PASSWORD and SFDC_SECURITY_TOKEN):
        raise RuntimeError("Set SFDC_CONSUMER_KEY+SFDC_CONSUMER_SECRET (Connected App) "
                           "or SFDC_USERNAME+SFDC_PASSWORD+SFDC_SECURITY_TOKEN")
    return Salesforce(
        username=SFDC_USERNAME,
        password=SFDC_PASSWORD,
        security_token=SFDC_SECURITY_TOKEN,
        domain=SFDC_DOMAIN,
    )


def fetch_sfdc(dots: list[str], policies: list[str]) -> dict:
    """
    Returns {
        "by_dot":    {dot:    opportunity_dict},
        "by_policy": {policy: opportunity_dict},
        "error":     str or None,
    }
    """
    if not dots and not policies:
        return {"by_dot": {}, "by_policy": {}, "error": None}

    try:
        sf = sfdc_client()
    except Exception as e:
        return {"by_dot": {}, "by_policy": {}, "error": f"SFDC auth failed: {e}"}

    where_clauses = []
    if dots:
        quoted_dots = ",".join("'" + d.replace("'", "\\'") + "'" for d in dots)
        where_clauses.append(f"{SFDC_DOT_FIELD} IN ({quoted_dots})")
    if policies:
        quoted_pols = ",".join("'" + p.replace("'", "\\'") + "'" for p in policies)
        where_clauses.append(f"{SFDC_POLICY_FIELD} IN ({quoted_pols})")

    soql = f"""
        SELECT {", ".join(SFDC_OPPORTUNITY_FIELDS)}
        FROM Opportunity
        WHERE {" OR ".join(where_clauses)}
    """

    try:
        records = sf.query_all(soql).get("records", [])
    except SalesforceError as e:
        return {"by_dot": {}, "by_policy": {}, "error": f"SFDC query failed: {e}"}
    except Exception as e:
        return {"by_dot": {}, "by_policy": {}, "error": f"SFDC query failed: {e}"}

    by_dot: dict[str, dict] = {}
    by_policy: dict[str, dict] = {}
    for r in records:
        flat = {
            "opportunity_name": r.get("Name"),
            "account_name":     (r.get("Account") or {}).get("Name"),
            "policy_number":    r.get(SFDC_POLICY_FIELD),
            "bd_owner":         (r.get("Owner") or {}).get("Name"),
            "bd_email":         (r.get("Owner") or {}).get("Email"),
            "stage":            r.get("StageName"),
            "gwp":              r.get("Amount"),
            "effective_date":   r.get("Effective_Date__c"),
            "close_date":       r.get("CloseDate"),
        }
        dot = (r.get(SFDC_DOT_FIELD) or "").strip() if isinstance(r.get(SFDC_DOT_FIELD), str) else None
        pol = (r.get(SFDC_POLICY_FIELD) or "").strip() if isinstance(r.get(SFDC_POLICY_FIELD), str) else None
        if dot:
            existing = by_dot.get(dot)
            if not existing or (flat.get("gwp") or 0) > (existing.get("gwp") or 0):
                by_dot[dot] = flat
        if pol:
            existing = by_policy.get(pol)
            if not existing or (flat.get("gwp") or 0) > (existing.get("gwp") or 0):
                by_policy[pol] = flat

    return {"by_dot": by_dot, "by_policy": by_policy, "error": None}


# ─── GROUPING ──────────────────────────────────────────────────────────────────

def fallback_name(ticket: dict) -> str:
    summary = (ticket.get("summary") or "").strip()
    return summary.split(" - ")[0][:60] or f"DOT {ticket.get('dot') or 'unknown'}"


def group_into_lanes(tickets, sfdc):
    """
    Groups tickets into accounts using Opportunity Name as the primary key
    (fallback chain: opportunity_name → AL policy → DOT → ticket key).

    Pre-pass: backfills Opportunity Name across tickets that share an AL Policy
    Number. Older tickets often have summary-only data (no Opportunity Name),
    while newer tickets on the same policy have it populated — without this
    backfill, the same account would split into two rows.
    """
    by_dot    = sfdc.get("by_dot", {})
    by_policy = sfdc.get("by_policy", {})
    accounts  = {}

    # Backfill Opportunity Name across tickets sharing an AL policy.
    policy_to_opp = {}
    for t in tickets:
        if t.get("al_policy") and t.get("opportunity_name"):
            policy_to_opp.setdefault(t["al_policy"], t["opportunity_name"])
    for t in tickets:
        if not t.get("opportunity_name") and t.get("al_policy"):
            backfilled = policy_to_opp.get(t["al_policy"])
            if backfilled:
                t["opportunity_name"] = backfilled

    for t in tickets:
        key = (t.get("opportunity_name")
               or t.get("al_policy")
               or t.get("dot")
               or f"ISSUE-{t.get('key')}")

        if key not in accounts:
            sf = by_dot.get(t["dot"]) if t.get("dot") else None
            if not sf and t.get("al_policy"):
                sf = by_policy.get(t["al_policy"])
            sf = sf or {}

            accounts[key] = {
                "key":              key,
                "opportunity_name": t.get("opportunity_name") or sf.get("account_name") or sf.get("opportunity_name") or fallback_name(t),
                "dot":              t.get("dot"),
                "al_policy":        t.get("al_policy"),
                "gl_policy":        t.get("gl_policy"),
                "mtc_policy":       t.get("mtc_policy"),
                "policy_number":    t.get("al_policy") or sf.get("policy_number"),
                "bd_owner":         sf.get("bd_owner") or "Unassigned",
                "bd_email":         sf.get("bd_email"),
                "stage":            sf.get("stage"),
                "gwp":              sf.get("gwp"),
                "effective_date":   sf.get("effective_date"),
                "tickets":          [],
                "lane_counts":      {LANE_CANCELLATION: 0, LANE_NONRENEWAL: 0,
                                     LANE_CONDITIONAL: 0, LANE_OTHER: 0},
                "oldest_age":       0,
            }

        acct = accounts[key]
        # Backfill missing identifiers as new tickets reveal them.
        for field_name in ("dot", "al_policy", "gl_policy", "mtc_policy"):
            if not acct.get(field_name) and t.get(field_name):
                acct[field_name] = t[field_name]
        if not acct.get("policy_number") and t.get("al_policy"):
            acct["policy_number"] = t["al_policy"]

        acct["tickets"].append(t)
        acct["lane_counts"][t["lane"]] += 1
        if (t["age_days"] or 0) > acct["oldest_age"]:
            acct["oldest_age"] = t["age_days"] or 0

    severity = [LANE_CANCELLATION, LANE_NONRENEWAL, LANE_CONDITIONAL, LANE_OTHER]
    lanes = {lane: [] for lane in severity}
    for acct in accounts.values():
        primary = next((l for l in severity if acct["lane_counts"][l] > 0), LANE_OTHER)
        acct["primary_lane"] = primary

        # Determine the most-urgent ticket in the primary lane and surface its
        # status on the account row so BDs can see workflow stage at a glance.
        primary_tickets = [t for t in acct["tickets"] if t["lane"] == primary]
        primary_tickets.sort(key=lambda t: (
            status_urgency(t.get("status")),
            -(t.get("age_days") or 0),
        ))
        if primary_tickets:
            top = primary_tickets[0]
            acct["primary_status"]      = top.get("status")
            acct["primary_status_tone"] = top.get("status_tone")
        else:
            acct["primary_status"]      = None
            acct["primary_status_tone"] = "neutral"

        lanes[primary].append(acct)

    # Sort within lane: urgent statuses first, then oldest age.
    tone_priority = {"urgent": 0, "waiting": 1, "active": 2, "neutral": 3}
    for items in lanes.values():
        items.sort(key=lambda a: (
            tone_priority.get(a.get("primary_status_tone"), 9),
            -(a.get("oldest_age") or 0),
            -sum(a["lane_counts"].values()),
        ))

    return lanes


# ─── FASTAPI ───────────────────────────────────────────────────────────────────

app = FastAPI(title="Bound Policies (BD)")
ROOT = Path(__file__).parent


@app.get("/api/data")
def get_data():
    try:
        raw_tickets = fetch_all_open_flt()
    except requests.HTTPError as e:
        raise HTTPException(502, f"Jira fetch failed: {e.response.status_code} {e.response.text[:200]}")
    except Exception as e:
        raise HTTPException(500, f"Jira fetch failed: {e}")

    tickets = [normalize_ticket(t) for t in raw_tickets]

    # SFDC is intentionally skipped — BD assignment and account context live in
    # the parent Bound Policies portal. Our embed only renders Jira-side work.
    sfdc = {"by_dot": {}, "by_policy": {}, "error": None}
    lanes = group_into_lanes(tickets, sfdc)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lanes":        lanes,
        "totals": {
            "accounts": sum(len(items) for items in lanes.values()),
            "tickets":  len(tickets),
            "by_lane":  {lane: len(items) for lane, items in lanes.items()},
        },
    }


_SUMMARY_CACHE = {}  # {issue_key: {"updated": str, "summary": str}}


def adf_to_text(node):
    """Recursively extract plain text from Atlassian Document Format."""
    if not isinstance(node, dict):
        return ""
    parts = []
    if node.get("type") == "text":
        parts.append(node.get("text", ""))
    for child in node.get("content") or []:
        parts.append(adf_to_text(child))
    if node.get("type") in ("paragraph", "heading", "listItem"):
        parts.append("\n")
    return "".join(parts)


def fetch_issue_for_summary(issue_key):
    resp = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}",
        headers=jira_auth_header(),
        params={
            "fields": ",".join([
                "summary", "status", "description", "comment", "assignee",
                "reporter", "creator", "attachment",
                "created", "updated", "issuetype", "priority", "duedate",
                F_NOTICE_TYPE, F_OPPORTUNITY_NAME,
                F_AL_POLICY, F_GL_POLICY, F_MTC_POLICY,
                F_DOT_NUMBER, F_EFFECTIVE, F_WORDING_FOR_NOTICE,
            ]),
            "expand": "changelog",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def build_summary_context(issue):
    fields = issue.get("fields", {})
    summary = fields.get("summary") or ""
    status = (fields.get("status") or {}).get("name") or ""
    issue_type = (fields.get("issuetype") or {}).get("name") or ""
    notice_type = get_option_value(fields.get(F_NOTICE_TYPE)) or ""
    opp_name    = fields.get(F_OPPORTUNITY_NAME) or ""
    dot         = fields.get(F_DOT_NUMBER) or ""
    al_policy   = fields.get(F_AL_POLICY) or ""
    effective   = fields.get(F_EFFECTIVE) or fields.get("duedate") or ""
    description = adf_to_text(fields.get("description")).strip() or "(no description provided)"

    # Whether the description appears to contain ANY agency-origin signal.
    # The model uses this as a hard pre-check; if false, it must NOT say
    # "the agency requested" no matter what the comments contain.
    desc_has_external_signal = any(
        marker in description.lower()
        for marker in [
            "rpsins.com", "usi.com", "amwins.com", "transtarinsurance.com",
            "crcgroup.com", "lockton.com", "marsh.com", "aon.com", "mcgriff.com",
            "@hubinternational.com", "@nfp.com", "cottinghambutler.com",
            "our mutual customer", "our insured",
            "risk placement services", "transtar insurance", "crc specialty",
            "amwins", "truck writers", "hub international",
        ]
    )

    comments = (fields.get("comment") or {}).get("comments", [])
    comment_blocks = []
    for c in comments[-15:]:  # last 15 comments to keep prompt size sane
        author = (c.get("author") or {}).get("displayName") or "Unknown"
        date = (c.get("created") or "")[:10]
        body = adf_to_text(c.get("body")).strip()
        if body:
            comment_blocks.append(f"[{author} on {date}]: {body}")

    histories = (issue.get("changelog") or {}).get("histories", [])
    status_changes = []
    for h in histories:
        date = (h.get("created") or "")[:10]
        author = (h.get("author") or {}).get("displayName") or "system"
        for item in h.get("items") or []:
            if item.get("field") == "status":
                status_changes.append(
                    f"{date}: {author} moved {item.get('fromString')} → {item.get('toString')}"
                )
    status_changes = status_changes[-8:]

    meaning = status_meaning(status)

    # Attachment filenames often disambiguate vague email bodies
    # (e.g., 'AL PD - Change Request - Amend Address.pdf' → address change).
    attachments = []
    for a in fields.get("attachment") or []:
        name = a.get("filename") or ""
        # Skip noisy embedded email images.
        if name.lower().startswith("image") and name.lower().endswith((".png", ".jpg", ".jpeg", ".gif")):
            continue
        if name:
            attachments.append(name)
    attachments_line = ", ".join(attachments[:8]) if attachments else "(none)"

    return (
        f"Title: {summary}\n"
        f"Type: {issue_type}\n"
        f"Description origin signal: {'EXTERNAL — the description contains an agency name, broker domain, or broker phrase' if desc_has_external_signal else 'INTERNAL — description has no external agency markers (empty, brief, or internal-only content)'}\n"
        f"Attachments: {attachments_line}\n"
        f"Notice Type (CANCELLATION/NONRENEWAL REASON — use exactly this, do "
        f"not invent another reason): {notice_type or '(not specified)'}\n"
        f"Opportunity / Account: {opp_name or '(not specified)'}\n"
        f"AL Policy Number: {al_policy or '(not specified)'}\n"
        f"DOT #: {dot or '(not specified)'}\n"
        f"Effective / Cancel Date: {effective or '(not specified)'}\n"
        f"Current Internal Status: {status}\n"
        f"Status meaning (GROUND TRUTH — anchor your summary here): "
        f"{meaning or '(no plain-English meaning available — describe based on description and comments)'}\n\n"
        f"Description:\n{description}\n\n"
        f"Recent comments (oldest → newest):\n"
        + ("\n".join(comment_blocks) if comment_blocks else "(no comments)")
        + "\n\nRecent status changes:\n"
        + ("\n".join(status_changes) if status_changes else "(none)")
    )


SUMMARY_SYSTEM_PROMPT = (
    "You write very short, plain-English summaries for a Business Development "
    "rep at Nirvana, a commercial-trucking insurance company. The reader "
    "manages the account but does NOT have access to any internal tools, "
    "ticket systems, or technical dashboards.\n\n"
    "FIRST — IDENTIFY WHAT THIS TICKET IS:\n"
    "Look at the 'Type' line in the context. Each ticket type has its own "
    "purpose. Frame the summary around THIS ticket's type — never describe "
    "this ticket as if it were a different kind of work just because other "
    "tickets on the same policy are different types.\n\n"
    "Common FLT ticket types and what each is about:\n"
    "- 'Notice Issuance -Cancelation' — issuing/rescinding a cancellation "
    "  notice. Apply the cancellation rules below.\n"
    "- 'Notice Issuance-Conditional or Nonrenewal' — issuing a nonrenewal or "
    "  conditional renewal notice. Apply the cancellation/nonrenewal rules "
    "  below.\n"
    "- 'Endorsement Request' — a change to an existing policy (coverage "
    "  change, address change, mailing change, etc). Cover: what change is "
    "  requested, current state, what's next. The cancellation rules do NOT "
    "  apply.\n"
    "- 'Vehicle Add/Delete' / 'Unit' — adding or removing a vehicle from the "
    "  policy. Cover: which unit, current state, what's next.\n"
    "- 'Driver Add' / 'Driver Review' — adding a driver or reviewing a "
    "  driver's eligibility. Cover: which driver, current state, what's "
    "  next.\n"
    "- 'Loss Runs' — request for prior loss-run reports. Cover: who "
    "  requested, current state, what's next.\n"
    "- 'Filings' — state filings (MCS-90, Form E, BMC-91, etc.). Cover: what "
    "  filing, current state, what's next.\n"
    "- 'Policy Inquiry' — a general question about the policy. Cover: the "
    "  question, current state, what's next.\n"
    "- 'Renewal Review' — review of an upcoming renewal. Cover: status of "
    "  the review, what's next.\n"
    "- 'Binding Request' — request to bind a new policy. Cover: current "
    "  state, what's next.\n"
    "- 'Camera Requirement' — telematics camera install or compliance check. "
    "  Cover: requirement, current state, what's next.\n"
    "- 'Claims Inquiry' — a question about a claim. Cover: the question, "
    "  current state, what's next.\n"
    "- 'Telematics Anomaly' — issue with telematics data (mileage, units). "
    "  Cover: the anomaly, current state, what's next.\n"
    "- 'Rescission/Reinstatement' — undoing a previous cancellation. Apply "
    "  rescission rules below.\n"
    "- 'Misc' / 'General request' — something that doesn't fit the others. "
    "  Just describe what's being requested in plain English.\n\n"
    "Cancellation-specific rules (RESCISSION, CANCELLATION REASONS, etc) "
    "below apply ONLY to tickets of type 'Notice Issuance -Cancelation', "
    "'Notice Issuance-Conditional or Nonrenewal', or 'Rescission/"
    "Reinstatement'. For other types, ignore those rules and write a simple "
    "summary about the request itself.\n\n"
    "OUTPUT FORMAT (must be valid JSON with EXACTLY these three keys):\n\n"
    "1. original_request (<= 25 words):\n"
    "   USE ONLY: the Description field, the Title, and the Attachments. "
    "DO NOT use comments — comments often contain formal notice templates "
    "that LOOK like external emails but are internal communication.\n\n"
    "   ABSOLUTE RULE — COLLECTIONS = BILLING, NEVER UNDERWRITING:\n"
    "   If the Description contains 'Cxl Per Collections Wkbk', 'Per "
    "Collections', 'Collections Wkbk', 'Per AR', 'Per Billing', or any "
    "phrase containing 'collections' (case-insensitive), the original "
    "request comes from OUR BILLING TEAM — never underwriting. The "
    "original_request must read: 'Our billing team flagged this for "
    "cancellation due to nonpayment.' This is the most common attribution "
    "mistake — always double-check before finalizing this field.\n\n"
    "   ATTRIBUTION DECISION TREE (follow exactly):\n"
    "   STEP 1 — Look at the 'Description origin signal' line in the "
    "context.\n"
    "   * If it says 'EXTERNAL' → the description has clear agency markers. "
    "Attribute to 'The agency' (you may name the agency if obvious from "
    "signature, e.g., 'Risk Placement Services').\n"
    "   * If it says 'INTERNAL' → DO NOT say 'the agency' under any "
    "circumstances. The ticket is internally originated. Go to Step 2.\n\n"
    "   STEP 2 — For INTERNAL tickets, attribute based on the ticket Type "
    "and any internal markers:\n"
    "   * 'Cxl Per Collections Wkbk', 'Per Collections', 'Per AR' (anywhere "
    "in description) → 'Our billing team flagged this for cancellation due "
    "to nonpayment.'\n"
    "   * Notice Issuance-Conditional or Nonrenewal (no external signal) → "
    "'Our underwriting team initiated a [conditional renewal / nonrenewal] "
    "notice for [account name].' Pick conditional vs nonrenewal based on "
    "the Notice Type field, the Title, or the Attachments.\n"
    "   * Notice Issuance -Cancelation (no external signal, not collections) "
    "→ 'Our underwriting team flagged this for cancellation.'\n"
    "   * Endorsement Request, Vehicle Add/Delete, Driver Add, etc. (no "
    "external signal) → describe the change neutrally without naming a "
    "source. e.g., 'Mailing address change for [account].' DO NOT say 'the "
    "agency requested' if origin is INTERNAL.\n"
    "   * Any other type with INTERNAL origin → describe the work neutrally "
    "or attribute to 'Our team' if needed.\n\n"
    "   EXTRACT SPECIFICS: Even when origin is internal, extract WHAT is "
    "being requested from the Title and Attachments. The email body is "
    "often vague ('Please process the attached'), while the Title and "
    "attachment filenames spell out the actual change ('Mailing Address "
    "Change', 'Amend Address.pdf', 'Add Driver').\n\n"
    "   The agency types that may appear in EXTERNAL descriptions:\n"
    "   (a) the EXTERNAL agency/broker — most descriptions are forwarded "
    "emails. Look for these strong signals of external origin:\n"
    "       * Email signature block with an external brokerage name (e.g. "
    "Transtar Insurance, USI, AmWINS, RPS, Risk Placement Services, Truck "
    "Writers, NFP, Lockton, Marsh, Aon, McGriff, HUB, Cottingham & Butler)\n"
    "       * External email domain in the signature (e.g. "
    "@transtarinsurance.com, @usi.com, @amwins.com — anything that is NOT "
    "@nirvanatech.com)\n"
    "       * Phrases like 'our mutual customer', 'our insured', 'my "
    "client' — agents/brokers say this; internal teams don't\n"
    "       * 'Senior Account Manager', 'Account Executive', or similar "
    "broker job titles in the signature\n"
    "       IMPORTANT: forwarding metadata like 'via Nirvana Underwriting "
    "Team' or 'via uw@nirvanatech.com' in the From: line is just the intake "
    "address — NOT the original sender. Look at the BOTTOM of the email "
    "(signature) to find the actual sender.\n"
    "   (b) Nirvana's INTERNAL billing/collections team — short jargon like "
    "'Cxl Per Collections Wkbk', 'Per Collections', 'Per AR'. No email "
    "signature, no external company name.\n"
    "   (c) Nirvana's INTERNAL underwriting team — short jargon like 'UW "
    "Decline', 'Per UW', 'UW reasons', 'Underwriter declined'. No external "
    "signature.\n"
    "   (d) Nirvana's INTERNAL compliance team — phrases like 'Per audit', "
    "'Compliance'. No external signature.\n"
    "   DECISION RULE: If the description has any external signature block, "
    "external company name in the signature, or phrases like 'our mutual "
    "customer' — the source is the AGENCY. Default to internal sources only "
    "when the description is short jargon with no email signature.\n"
    "   Examples of correct attribution:\n"
    "   * 'The agency requested a cancellation notice for nonpayment of "
    "February 2026 premium.'\n"
    "   * 'Our billing team flagged this for cancellation due to nonpayment.'\n"
    "   * 'Our underwriting team decided to non-renew this policy.'\n"
    "   If the description is empty or unintelligible, return an empty "
    "string.\n\n"
    "2. latest_note (<= 25 words):\n"
    "   The most recent SUBSTANTIVE internal comment — something that adds "
    "NEW information beyond what the status already conveys. Examples: "
    "'Agency confirmed payment received', 'Insured promised wire on Friday', "
    "'Underwriter approved a 2-week extension', 'Falls Lake confirmed "
    "reinstatement'.\n"
    "   Skip non-substantive notes that just restate the status (e.g. 'we "
    "are rescinding the cancellation' is just the status — NOT a real note). "
    "If there are no substantive comments, return an empty string. Empty is "
    "better than echoing the status.\n\n"
    "3. whats_next (<= 30 words):\n"
    "   Anchor on the Status meaning. State what is happening now and what "
    "the next step is. Follow all the terminology, workflow, rescission, "
    "cancellation-reason, urgency, and banned-words rules below. This is the "
    "most important field.\n\n"
    "Return ONLY the JSON object — no markdown code fences, no extra keys.\n\n"
    "GROUND TRUTH:\n"
    "The 'Status meaning' line in the context is the authoritative description "
    "of where the work currently stands. Anchor your summary on it — do not "
    "contradict it. The description, comments, and history are supporting "
    "detail to flesh out WHY this happened and any specific next step.\n\n"
    "WHO NIRVANA IS:\n"
    "Nirvana IS the insurance carrier. When you say 'we' or 'Nirvana', that is "
    "the carrier. Do NOT refer to Nirvana as 'the carrier' (self-reference is "
    "confusing).\n\n"
    "TERMINOLOGY (strict — never confuse these):\n"
    "- 'we' / 'our team' / 'Nirvana' = us. This INCLUDES any contractors we "
    "  use behind the scenes — refer to all such work as 'our team' or 'we'. "
    "  Never name internal contractors.\n"
    "- 'the agent' / 'the agency' = the EXTERNAL broker or agency partner (USI, "
    "  AmWINS, Truck Writers, Risk Placement Services, etc.). NEVER use these "
    "  to refer to anyone at Nirvana.\n"
    "- 'the underwriter' = a Nirvana underwriter (internal).\n"
    "- 'the insured' = the trucking company being insured (the policyholder).\n\n"
    "═══════════════════════════════════════════════════════════════\n"
    "TYPE-SPECIFIC RULES — applied based on the ticket's Type field\n"
    "═══════════════════════════════════════════════════════════════\n\n"
    "▸ CANCELLATION / NONRENEWAL / CONDITIONAL TICKETS\n"
    "  (Type = 'Notice Issuance -Cancelation', 'Notice Issuance-"
    "Conditional or Nonrenewal', or 'Rescission/Reinstatement')\n\n"
    "WORKFLOW DIRECTION (cancellations and notices):\n"
    "- Our team prepares the cancellation/nonrenewal/conditional notice.\n"
    "- We send the notice TO the agency.\n"
    "- The agency relays the notice to the insured.\n"
    "- The insured may pay or cure the issue; otherwise the cancellation takes "
    "  effect on the cancel date.\n"
    "- Notices flow OUT FROM us TO the agency. Never say notices are sent "
    "  'to the carrier' — Nirvana IS the carrier.\n\n"
    "MAIL BY DATE (nonrenewal and conditional tickets):\n"
    "- The renewal/conditional notice must be mailed to the agency by a "
    "  specific date called the 'Mail By' date. This is usually in the "
    "  Title (e.g., 'HYPE TOY... - Conditional - Mail By 6/3/26').\n"
    "- For nonrenewal and conditional tickets, ALWAYS mention the Mail By "
    "  date in 'whats_next' if it's present in the Title or Description. "
    "  Phrase it as 'The renewal notice will be sent around <date>.'\n"
    "- If no Mail By date is visible, just describe what's pending without "
    "  inventing one.\n\n"
    "RESCISSION (very important — easy to get wrong):\n"
    "- 'Rescission' / 'rescind' / 'Rescission Needed' / 'Ready to rescind' all "
    "  refer to UNDOING a previously-issued cancellation notice. This happens "
    "  when the insured cures the underlying issue before the cancel date.\n"
    "- A rescission means the cancellation will NOT take effect and the policy "
    "  STAYS IN FORCE. It is GOOD news for the account.\n"
    "- NEVER write 'the account is being rescinded' or 'the policy is being "
    "  rescinded' — that means the opposite of what's happening. Write 'the "
    "  cancellation is being rescinded' or 'we are rescinding the cancellation "
    "  notice — the policy stays in force'.\n\n"
    "CANCELLATION REASONS (very strict — do NOT assume nonpayment):\n"
    "- The 'Notice Type' line in the context is the ONLY authoritative source "
    "  for why a cancellation/nonrenewal/conditional was issued. Possible "
    "  values: nonpayment of premium, insured request, underwriting reasons, "
    "  non-compliance, LOC (letter of credit) not received, insured out of "
    "  business.\n"
    "- If Notice Type is '(not specified)' or empty, DO NOT mention any "
    "  specific reason. Do NOT infer or guess from the description, comments, "
    "  or ticket summary. Just say 'the cancellation' or 'this cancellation' "
    "  without specifying why.\n"
    "- Internal jargon like 'Collections', 'Cxl Per Collections Wkbk', 'UW "
    "  Decline', 'LOC missing', etc. is NOT a substitute for Notice Type. "
    "  Don't translate those phrases into reasons. If the structured Notice "
    "  Type field isn't set, the reason is unknown — say nothing about it.\n"
    "- For RESCISSION summaries specifically: do NOT mention the original "
    "  cancellation reason at all. It's being undone — the original reason is "
    "  no longer relevant. Focus only on the rescission itself and that the "
    "  policy stays in force.\n\n"
    "WHO IS DOING WHAT:\n"
    "- ONLY mention parties whose involvement is EXPLICITLY in the context.\n"
    "- Don't invent work for the agency or the insured if the context doesn't "
    "  show it.\n\n"
    "URGENCY LANGUAGE (do NOT use any of these):\n"
    "- 'timely action', 'urgent', 'as soon as possible', 'must be completed', "
    "  'needs to be done by', 'time-sensitive', 'priority', 'immediately', "
    "  'as required', 'critical'.\n"
    "- Stating a date is fine. Do NOT add pressure language around the date.\n\n"
    "BANNED WORDS / NAMES (never use any of these):\n"
    "- 'Flatworld' (this is an internal contractor BDs should never see).\n"
    "- 'ticket', 'issue', 'Jira', 'status', 'assignee', 'queue', 'reporter', "
    "  'epic', 'sprint', 'transition', 'comment'.\n\n"
    "FORMATTING (strict):\n"
    "- Maximum 2-3 short sentences. Around 40-60 words total.\n"
    "- Plain English. No internal acronyms.\n"
    "- No bullet points. No headings. Just plain prose.\n"
    "- Do not start with 'This account' or 'The summary'. Lead with the fact."
)

# Bumping this invalidates all cached summaries the next time they're requested.
# Increment when you change the system prompt or the context-building logic.
SUMMARY_PROMPT_VERSION = "v19"


def generate_summary(context):
    """
    Returns a dict with three keys: agency_request, latest_note, whats_next.
    Falls back to {whats_next: <raw text>} if the LLM returns malformed JSON.
    """
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set in .env")
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.3,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
            {"role": "user",   "content": context},
        ],
    )
    raw = (resp.choices[0].message.content or "").strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"original_request": "", "latest_note": "", "whats_next": raw}
    return {
        "original_request": (parsed.get("original_request") or "").strip(),
        "latest_note":      (parsed.get("latest_note") or "").strip(),
        "whats_next":       (parsed.get("whats_next") or "").strip(),
    }


@app.get("/api/summary/{issue_key}")
def get_summary(issue_key: str):
    try:
        issue = fetch_issue_for_summary(issue_key)
    except requests.HTTPError as e:
        raise HTTPException(502, f"Couldn't fetch {issue_key}: {e.response.status_code} {e.response.text[:200]}")
    except Exception as e:
        raise HTTPException(500, f"Couldn't fetch {issue_key}: {e}")

    fields = issue.get("fields") or {}
    updated   = fields.get("updated", "")
    assignee  = (fields.get("assignee") or {}).get("displayName") or "Unassigned"
    cache_key = f"{SUMMARY_PROMPT_VERSION}:{updated}"
    cached    = _SUMMARY_CACHE.get(issue_key)

    # Wording-for-notice (only meaningful for cancellation/nonrenewal/
    # conditional tickets; lives in customfield_10150 as ADF).
    notice_wording = adf_to_text(fields.get(F_WORDING_FOR_NOTICE)).strip()

    # Build the attachment list (filter out inline email images).
    attachments = []
    for a in fields.get("attachment") or []:
        name = a.get("filename") or ""
        if not name:
            continue
        if name.lower().startswith("image") and name.lower().endswith((".png", ".jpg", ".jpeg", ".gif")):
            continue
        attachments.append({
            "id":        a.get("id"),
            "filename":  name,
            "size":      a.get("size") or 0,
            "mime_type": a.get("mimeType") or "",
            "created":   a.get("created"),
        })

    if cached and cached.get("cache_key") == cache_key:
        return {
            "issue_key":         issue_key,
            "original_request":  cached["parts"].get("original_request", ""),
            "latest_note":       cached["parts"].get("latest_note", ""),
            "whats_next":        cached["parts"].get("whats_next", ""),
            "currently_with":    assignee,
            "notice_wording":    notice_wording,
            "attachments":       attachments,
            "cached":            True,
        }

    try:
        parts = generate_summary(build_summary_context(issue))
    except Exception as e:
        raise HTTPException(500, f"Summary generation failed: {e}")

    _SUMMARY_CACHE[issue_key] = {"cache_key": cache_key, "parts": parts}
    return {
        "issue_key":         issue_key,
        "original_request":  parts.get("original_request", ""),
        "latest_note":       parts.get("latest_note", ""),
        "whats_next":        parts.get("whats_next", ""),
        "currently_with":    assignee,
        "notice_wording":    notice_wording,
        "attachments":       attachments,
        "cached":            False,
    }


def bd_portal_auth_header():
    if not (BD_PORTAL_EMAIL and BD_PORTAL_TOKEN):
        raise HTTPException(500, "BD_PORTAL_EMAIL and BD_PORTAL_TOKEN must be set in .env")
    raw = f"{BD_PORTAL_EMAIL}:{BD_PORTAL_TOKEN}".encode()
    return {
        "Authorization": f"Basic {base64.b64encode(raw).decode()}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_bd_portal_account_id():
    """Returns the BD Portal account's Atlassian accountId (set in .env after one-time customer creation)."""
    if not BD_PORTAL_ACCOUNT_ID:
        raise HTTPException(500, "BD_PORTAL_ACCOUNT_ID not set in .env — see setup script")
    return BD_PORTAL_ACCOUNT_ID


def add_bd_portal_as_participant(issue_key):
    """
    Adds BD Portal as a request participant on the given ticket so it can post
    comments. Idempotent — Jira returns 200 if already a participant, or
    silently no-ops. Uses the agent token (Justine's) since participant
    management is an agent-level action.
    """
    account_id = get_bd_portal_account_id()
    resp = requests.post(
        f"{JIRA_BASE_URL}/rest/servicedeskapi/request/{issue_key}/participant",
        headers={**jira_auth_header(), "Content-Type": "application/json"},
        json={"accountIds": [account_id]},
        timeout=15,
    )
    # 200 OK or 400 "already a participant" both mean success for our purposes.
    if resp.status_code not in (200, 400):
        # Don't hard-fail — sometimes the participant endpoint isn't available
        # for non-portal-created tickets. We'll still try to post the comment.
        pass


@app.post("/api/comment/{issue_key}")
def post_comment(issue_key: str, payload: dict):
    """
    Posts a BD-relayed comment to a Jira ticket as a PUBLIC ('Reply to
    customer') comment using the agent token. The body is prefixed with the
    BD's name so Ops can tell which BD asked. Internal Ops notes stay
    separate because they use a different comment type (jsdPublic=false).

    Request body: { "bd_name": "Justine Nazarro", "body": "comment text" }
    """
    bd_name  = (payload.get("bd_name") or "").strip()
    raw_body = (payload.get("body") or "").strip()
    if not bd_name:
        raise HTTPException(400, "bd_name is required")
    if not raw_body:
        raise HTTPException(400, "body is required")

    body_with_prefix = f"[{bd_name} (BD)]: {raw_body}"

    resp = requests.post(
        f"{JIRA_BASE_URL}/rest/servicedeskapi/request/{issue_key}/comment",
        headers={**jira_auth_header(), "Content-Type": "application/json"},
        json={"body": body_with_prefix, "public": True},
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        raise HTTPException(
            502,
            f"Comment post failed ({resp.status_code}): {resp.text[:300]}",
        )

    data = resp.json()
    return {
        "ok":         True,
        "comment_id": data.get("id"),
        "created":    data.get("created", {}).get("iso8601") if isinstance(data.get("created"), dict) else data.get("created"),
        "issue_key":  issue_key,
    }


@app.get("/api/notifications/{bd_name}")
def get_notifications(bd_name: str):
    """
    Returns recent Ops replies on threads this specific BD has posted on.
    Used by the notification bell in the top bar.

    Strategy: search Jira for tickets where any comment matches the BD's
    prefix '[Name (BD)]:'. For each match, fetch the BD-conversation
    comments (already filtered) and return the non-BD (Ops) comments —
    those are the replies the BD might want to know about.
    """
    bd_name = (bd_name or "").strip()
    if not bd_name:
        return {"bd_name": "", "notifications": []}

    safe = bd_name.replace('"', '').replace("\\", "")
    # JQL's `~` tokenizer strips out brackets and parentheses, so we use an
    # inner-quoted phrase to force exact phrase matching on '<name> (BD)'.
    # The escaped inner quotes are interpreted by JQL as a phrase boundary.
    jql = (
        f'project = FLT AND comment ~ "\\"{safe} (BD)\\"" '
        f'AND updated > -30d ORDER BY updated DESC'
    )

    try:
        issues = fetch_jira_tickets_with_jql(jql, max_pages=2)
    except Exception:
        return {"bd_name": bd_name, "notifications": []}

    notifications = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(fetch_public_comments_for_issue, i["key"]): i for i in issues}
        for f in futures:
            issue = futures[f]
            comments = f.result()
            normalized = normalize_ticket(issue)
            for c in comments:
                if c.get("is_bd"):
                    continue  # Skip the BD's own messages
                notifications.append({
                    "comment_id":       c["id"],
                    "issue_key":        issue["key"],
                    "issue_type":       normalized.get("issue_type"),
                    "issue_summary":    normalized.get("summary"),
                    "policy_number":    normalized.get("al_policy"),
                    "opportunity_name": normalized.get("opportunity_name"),
                    "dot":              normalized.get("dot"),
                    "author_name":      c["author_name"],
                    "body":             c["body"][:240],
                    "created":          c["created"],
                })

    notifications.sort(key=lambda n: n["created"] or "", reverse=True)
    return {"bd_name": bd_name, "notifications": notifications[:50]}


@app.get("/api/policy/{policy_key}/comms")
def get_policy_comms(policy_key: str):
    """
    Returns the BD ↔ Ops conversation threads across ALL tickets on this
    policy — open AND closed. The Comms tab is the historical archive of
    every BD-Ops exchange tied to this account, surviving past ticket
    closure. Internal Ops notes are excluded.
    """
    # Fetch tickets matching this policy regardless of status. Use cf[NNNNN]
    # JQL syntax for custom fields. Window to last 365d so we don't drag in
    # ancient archives but still capture the whole renewal cycle.
    safe_key = policy_key.replace('"', '\\"')
    jql = (
        f'project = FLT AND ('
        f'cf[10130] ~ "{safe_key}" OR '   # AL Policy Number
        f'cf[10184] = "{safe_key}" OR '    # DOT #
        f'cf[11202] ~ "{safe_key}" OR '    # Opportunity Name
        f'summary ~ "{safe_key}"'
        f') AND updated > -365d ORDER BY updated DESC'
    )

    try:
        raw_tickets = fetch_jira_tickets_with_jql(jql, max_pages=5)
    except Exception as e:
        raise HTTPException(500, f"Jira fetch failed: {e}")

    tickets = [normalize_ticket(t) for t in raw_tickets]

    # Backfill opportunity_name across tickets sharing AL policy.
    policy_to_opp = {}
    for t in tickets:
        if t.get("al_policy") and t.get("opportunity_name"):
            policy_to_opp.setdefault(t["al_policy"], t["opportunity_name"])
    for t in tickets:
        if not t.get("opportunity_name") and t.get("al_policy"):
            t["opportunity_name"] = policy_to_opp.get(t["al_policy"]) or t.get("opportunity_name")

    threads = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(fetch_public_comments_for_issue, t["key"]): t for t in tickets}
        for f in futures:
            t = futures[f]
            comments = f.result()
            # Skip threads that have no BD-relayed comments — those are
            # historical Ops↔broker exchanges (pre-app) that we shouldn't
            # surface in the BD's Comms archive.
            if not any(c.get("is_bd") for c in comments):
                continue
            threads.append({
                "issue_key":     t["key"],
                "issue_type":    t.get("issue_type"),
                "issue_summary": t.get("summary"),
                "status":        t.get("status"),
                "is_open":       (t.get("status") or "").lower() not in ("closed", "done", "cancelled", "resolved"),
                "lane":          t.get("lane"),
                "comments":      comments,
            })

    # Sort threads: ones with comments first, then by ticket key.
    threads.sort(key=lambda x: (-len(x["comments"]), x["issue_key"]))

    return {"policy_key": policy_key, "threads": threads}


@app.get("/api/attachment/{attachment_id}")
def get_attachment(attachment_id: str):
    """
    Proxies a Jira attachment download. BDs don't have Jira access, so we
    authenticate on their behalf and stream the file back.
    """
    meta_resp = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/attachment/{attachment_id}",
        headers=jira_auth_header(),
        timeout=30,
    )
    if meta_resp.status_code != 200:
        raise HTTPException(meta_resp.status_code, f"Attachment metadata fetch failed: {meta_resp.text[:200]}")

    meta = meta_resp.json()
    content_url = meta.get("content")
    if not content_url:
        raise HTTPException(404, "Attachment has no content URL")

    file_resp = requests.get(content_url, headers=jira_auth_header(), timeout=60, stream=True)
    if file_resp.status_code != 200:
        raise HTTPException(file_resp.status_code, f"Attachment content fetch failed: {file_resp.text[:200]}")

    filename = (meta.get("filename") or attachment_id).replace('"', '')
    media_type = file_resp.headers.get("Content-Type") or meta.get("mimeType") or "application/octet-stream"
    return StreamingResponse(
        file_resp.iter_content(chunk_size=8192),
        media_type=media_type,
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@app.get("/api/policies")
def get_policies():
    """
    Returns a flat list of policies that have open Jira work, with summary
    counts per lane. Used by the portal mock to drive the policy list.
    """
    try:
        raw_tickets = fetch_all_open_flt()
    except requests.HTTPError as e:
        raise HTTPException(502, f"Jira fetch failed: {e.response.status_code} {e.response.text[:200]}")
    except Exception as e:
        raise HTTPException(500, f"Jira fetch failed: {e}")

    tickets = [normalize_ticket(t) for t in raw_tickets]

    # Backfill opportunity_name across tickets sharing an AL Policy Number.
    policy_to_opp = {}
    for t in tickets:
        if t.get("al_policy") and t.get("opportunity_name"):
            policy_to_opp.setdefault(t["al_policy"], t["opportunity_name"])
    for t in tickets:
        if not t.get("opportunity_name") and t.get("al_policy"):
            backfilled = policy_to_opp.get(t["al_policy"])
            if backfilled:
                t["opportunity_name"] = backfilled

    severity = [LANE_CANCELLATION, LANE_NONRENEWAL, LANE_CONDITIONAL, LANE_OTHER]
    lane_priority = {l: i for i, l in enumerate(severity)}
    policies = {}

    for t in tickets:
        # Group by AL Policy Number first (most stable). Fallbacks: opp name, DOT.
        key = t.get("al_policy") or t.get("opportunity_name") or t.get("dot") or f"ISSUE-{t.get('key')}"
        if key not in policies:
            policies[key] = {
                "key":              key,
                "policy_number":    t.get("al_policy"),
                "opportunity_name": t.get("opportunity_name") or fallback_name(t),
                "dot":              t.get("dot"),
                "ticket_count":     0,
                "lane_counts":      {l: 0 for l in severity},
                "issue_types":      set(),
                "oldest_age":       0,
                "has_urgent":       False,
            }
        p = policies[key]
        if not p["dot"] and t.get("dot"):
            p["dot"] = t["dot"]
        p["ticket_count"] += 1
        p["lane_counts"][t["lane"]] += 1
        if t.get("issue_type"):
            p["issue_types"].add(t["issue_type"])
        if (t.get("age_days") or 0) > p["oldest_age"]:
            p["oldest_age"] = t["age_days"] or 0
        if t.get("status_tone") == "urgent":
            p["has_urgent"] = True

    # Convert sets → sorted lists for JSON serialization.
    for p in policies.values():
        p["issue_types"] = sorted(p["issue_types"])

    for p in policies.values():
        p["primary_lane"] = next(
            (l for l in severity if p["lane_counts"][l] > 0),
            LANE_OTHER,
        )

    policy_list = sorted(
        policies.values(),
        key=lambda p: (
            0 if p["has_urgent"] else 1,
            lane_priority[p["primary_lane"]],
            -(p.get("oldest_age") or 0),
        ),
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "policies":     policy_list,
        "totals": {
            "policies_with_open_work": len(policy_list),
            "total_tickets":           len(tickets),
            "by_lane": {
                l: sum(1 for p in policy_list if p["primary_lane"] == l)
                for l in severity
            },
        },
    }


import re
BD_PREFIX_RE = re.compile(r"^\s*\[([^\]]+?)\s*\(BD\)\]:\s*(.*)", re.DOTALL)


def fetch_public_comments_for_issue(issue_key):
    """
    Returns the BD-conversation comments on a ticket.

    Public (jsdPublic=true) comments include both:
    - BD-relayed messages (have [Name (BD)]: prefix)
    - Ops "Reply to customer" replies (no prefix, authored by Ops people)
    - Historical Ops-to-Flatworld/broker public comments that happened to be
      marked public but aren't BD-Ops conversations.

    To exclude the third category, we only return comments from the FIRST
    BD-prefixed comment onwards. Anything posted before the BD started a
    thread is treated as historical noise and dropped.
    """
    r = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}",
        headers=jira_auth_header(),
        params={"fields": "comment"},
        timeout=20,
    )
    if r.status_code != 200:
        return []
    raw = ((r.json().get("fields") or {}).get("comment") or {}).get("comments", [])
    parsed = []
    for c in raw:
        if not c.get("jsdPublic"):
            continue
        author = c.get("author") or {}
        body_text = adf_to_text(c.get("body")).strip()
        m = BD_PREFIX_RE.match(body_text)
        if m:
            parsed.append({
                "id":          c.get("id"),
                "author_name": f"{m.group(1).strip()} (BD)",
                "is_bd":       True,
                "body":        m.group(2).strip(),
                "created":     c.get("created"),
            })
        else:
            parsed.append({
                "id":          c.get("id"),
                "author_name": author.get("displayName") or "Unknown",
                "is_bd":       False,
                "body":        body_text,
                "created":     c.get("created"),
            })

    # Trim to BD-conversation only: drop anything before the first BD comment.
    first_bd_idx = next((i for i, c in enumerate(parsed) if c["is_bd"]), -1)
    if first_bd_idx == -1:
        return []
    return parsed[first_bd_idx:]


def fetch_jira_tickets_with_jql(jql, max_pages=8):
    """Generic paginated ticket fetcher with arbitrary JQL."""
    issues = []
    next_token = None
    for _ in range(max_pages):
        params = {
            "jql": jql,
            "fields": ",".join(JIRA_FIELDS),
            "maxResults": 100,
        }
        if next_token:
            params["nextPageToken"] = next_token
        resp = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            headers=jira_auth_header(),
            params=params,
            timeout=30,
        )
        if resp.status_code != 200:
            break
        page = resp.json()
        issues.extend(page.get("issues", []))
        next_token = page.get("nextPageToken")
        if not next_token or page.get("isLast"):
            break
    return issues


@app.get("/api/policy/{policy_key}/tickets")
def get_policy_tickets(policy_key: str):
    """All open Jira tickets for a single policy (or DOT), sorted by lane severity."""
    try:
        raw_tickets = fetch_all_open_flt()
    except requests.HTTPError as e:
        raise HTTPException(502, f"Jira fetch failed: {e.response.status_code} {e.response.text[:200]}")
    except Exception as e:
        raise HTTPException(500, f"Jira fetch failed: {e}")

    tickets = [normalize_ticket(t) for t in raw_tickets]

    # Backfill opportunity_name as in get_policies — needed for fallback name.
    policy_to_opp = {}
    for t in tickets:
        if t.get("al_policy") and t.get("opportunity_name"):
            policy_to_opp.setdefault(t["al_policy"], t["opportunity_name"])
    for t in tickets:
        if not t.get("opportunity_name") and t.get("al_policy"):
            t["opportunity_name"] = policy_to_opp.get(t["al_policy"]) or t.get("opportunity_name")

    matching = [t for t in tickets if t.get("al_policy") == policy_key]
    if not matching:
        matching = [t for t in tickets if t.get("dot") == policy_key]
    if not matching:
        matching = [t for t in tickets if t.get("opportunity_name") == policy_key]

    severity = [LANE_CANCELLATION, LANE_NONRENEWAL, LANE_CONDITIONAL, LANE_OTHER]
    lane_priority = {l: i for i, l in enumerate(severity)}
    matching.sort(key=lambda t: (
        lane_priority[t["lane"]],
        status_urgency(t.get("status")),
        -(t.get("age_days") or 0),
    ))

    # Fetch public comments for each ticket concurrently so the JiraTab can
    # render inline conversation threads on each card.
    with ThreadPoolExecutor(max_workers=5) as pool:
        future_to_idx = {
            pool.submit(fetch_public_comments_for_issue, t["key"]): i
            for i, t in enumerate(matching)
        }
        for f in future_to_idx:
            i = future_to_idx[f]
            matching[i]["comments"] = f.result()

    # On the Jira tab, only surface comment threads where the BD has actually
    # started a conversation (one or more comments has the [Name (BD)]:
    # prefix). Otherwise public Ops-internal-but-customer-visible comments
    # (auto-generated AI suggestions, broker handoff notes, etc.) leak into
    # the BD's "Conversation with Ops" view. If no BD conversation exists,
    # the card shows an empty thread and the reply box, ready for the BD
    # to start one.
    for t in matching:
        if not any(c.get("is_bd") for c in (t.get("comments") or [])):
            t["comments"] = []

    account = None
    if matching:
        first = matching[0]
        account = {
            "policy_number":    first.get("al_policy"),
            "opportunity_name": first.get("opportunity_name") or fallback_name(first),
            "dot":              first.get("dot"),
        }

    return {"policy_key": policy_key, "account": account, "tickets": matching}


@app.get("/portal")
def portal():
    return FileResponse(ROOT / "portal.html")


@app.get("/")
def index():
    return FileResponse(ROOT / "index.html")


@app.get("/healthz")
def health():
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
