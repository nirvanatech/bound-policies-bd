# Bound Policies (BD) — Standalone Prototype

A working web app that pulls open Fleet Jira tickets, joins them to Salesforce
opportunity data, and groups everything into four BD-focused priority lanes:

- **Cancellations Pending** — `Notice Type` starts with `Cancellation -`
- **Nonrenewals** — `Notice Type` = `Nonrenewal`
- **Conditional Notices** — `Notice Type` = `Conditional Renewal`
- **Additional Fleet Ops** — every other open FLT ticket

Built outside Nirvana AI so it runs against your own Jira/SFDC credentials and
is fully extensible — add new lanes, integrations, or capabilities the Nirvana
Apps platform doesn't support.

---

## Stack

- **Backend:** FastAPI (Python). Hits the Atlassian Jira REST API and the
  Salesforce REST API directly, joins on DOT # (with policy-number fallback),
  and returns one JSON payload.
- **Frontend:** Single `index.html` with React + Tailwind via CDN. No build
  step — the browser transpiles JSX with Babel Standalone. Inter typography,
  calm slate palette, color reserved for lane severity.

---

## Run it

```bash
cd standalone

# 1. Install deps
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure credentials
cp .env.example .env
# Edit .env — fill in JIRA_TOKEN, SFDC_PASSWORD, SFDC_SECURITY_TOKEN

# 3. Run
python app.py
```

Open `http://localhost:8000`.

**Getting credentials:**

- **Jira:** [id.atlassian.com → Security → API tokens](https://id.atlassian.com/manage-profile/security/api-tokens). Email is your work email.
- **Salesforce:** Salesforce → Settings → Reset My Security Token. Salesforce emails the token to you.

---

## Where to look in the code

| Need to change... | File / location |
|---|---|
| Add a new priority lane | `app.py` § `classify()` + `index.html` const `LANES` |
| Adjust SFDC field names | `app.py` § `SFDC_OPPORTUNITY_FIELDS` and `SFDC_DOT_FIELD` |
| Adjust Jira custom field IDs | `app.py` § `F_*` constants |
| Change "open" definition | `app.py` § `OPEN_JQL` |
| Adjust the stale threshold (`> 7 days`) | `index.html` § `LaneSubhead` (the `>= 7` check) |
| Change page size / pagination ceiling | `app.py` § `fetch_all_open_flt` (`page_size`, `range(1, 30)`) |

---

## Roadmap — capabilities Nirvana AI can't easily do

These are easy to add here because we own the stack:

1. **Persisted user identity & "My Accounts" default.** Nirvana Apps has no per-user state; here you can add a session cookie or auth header and default the BD filter to the logged-in user.
2. **Push notifications when a Cancellation is filed against an account they own.** Add a Slack webhook in `app.py` and a small worker that runs every N minutes.
3. **Inline Jira actions (transition status, add comment) without leaving the page.** Wire `index.html` to call `/api/jira/transition` endpoints we add in `app.py`.
4. **Custom views per BD** (saved filters, hidden lanes, pinned accounts). Persist to a small SQLite or Postgres alongside the FastAPI app.
5. **Cross-program rollup** (Fleet + Non-Fleet + Business Auto in one view). Loosen the JQL and add a program filter.
6. **Trend charts** (cancellations week-over-week, time-to-resolve by lane). Add `/api/trends` and a Recharts component.

---

## Pitching the prototype

When you demo to whoever has the right access, hit these in order:

1. **The lanes are the navigation.** Open the cancellations tab — show the count, oldest age, BD owner.
2. **One click drills into the actual work.** Expand a row → show the Jira tickets → click → opens Jira.
3. **Filter by BD.** Pick a name from the dropdown — show how a single BD sees only their book.
4. **Show what you'd add next** from the roadmap above. Frame it as "the platform we have today blocks #1 and #2; this prototype unblocks them."

---

## Deploying for stakeholders

When local dev isn't enough, the simplest temporary share is:

```bash
# In one terminal
python app.py

# In another
ngrok http 8000
```

Send the resulting `https://*.ngrok-free.app` URL. The viewer needs no install.
For a more permanent home, deploy to **Fly.io** (one-line deploy with `fly launch`)
or **Railway** (connect the repo).
