# tfg-datawarehouse

dbt project for the TFG Data Foundation — a unified canonical data layer built on Microsoft Fabric.

---

## Quick Start (Windows)

```powershell
# 1. Clone the repo
git clone https://github.com/datagrokr/tfg-datawarehouse.git
cd tfg-datawarehouse

# 2. Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Verify dbt installation
dbt --version

# 5. Set up your profile (see Profiles section below)

# 6. Compile to verify setup
dbt compile
```

---

## Project Structure

```
tfg-datawarehouse/
├── models/
│   ├── bronze/          # Raw ingestion — one model per source table, no transformations
│   │   ├── netsuite/
│   │   ├── adp/
│   │   ├── janit/
│   │   ├── servicechannel/
│   │   ├── utilizecore/
│   │   ├── springshot/
│   │   └── ebs/         # CONDITIONAL — Phase 04 only
│   ├── silver/          # Normalised, source-shaped — typed, renamed, cleaned
│   │   └── (mirrors bronze structure)
│   └── gold/            # Canonical domain entities — Power BI consumes this layer
├── macros/
├── seeds/
├── tests/
├── ARCHITECTURE.md      # Technical decision record — read before making stack changes
├── PHASES.md            # Model-to-phase map — read before starting any build work
├── dbt_project.yml
└── requirements.txt
```

---

## Layer Contracts

| Layer  | Rule                                                                 |
|--------|----------------------------------------------------------------------|
| Bronze | No transformations. Column names match source exactly.               |
| Silver | Light transforms only — type casting, snake_case rename, null handling. No cross-source joins. |
| Gold   | Canonical entities. Cross-source joins happen here. All canonical_id fields must be NOT NULL. |

---

## Build Phases

See `PHASES.md` for the full model-to-phase mapping. In summary:

- **Phase 02 (Wks 4–15):** NetSuite, ADP, Jan-IT — build these first
- **Phase 03 (Wks 16–22):** Service Channel, UtilizeCore, Springshot
- **Phase 04 (Post Q3/Q4):** Oracle EBS historical — CONDITIONAL, do not build until confirmed

---

## dbt Profile Setup

Add the following to your `~/.dbt/profiles.yml` (do not commit this file):

```yaml
tfg_datawarehouse:
  target: dev
  outputs:
    dev:
      type: fabric
      driver: "ODBC Driver 18 for SQL Server"
      server: <your-fabric-warehouse-endpoint>
      port: 1433
      database: <your-fabric-database>
      schema: dbt_<your_name>
      authentication: CLI
      encrypt: true
      trust_cert: false
```

> Confirm the Fabric Warehouse endpoint with Zeel or Keaton before filling this in.
> If TFG's Fabric is Lakehouse-only, a Warehouse must be provisioned first — see ARCHITECTURE.md.

---

## Bronze Ingestion Pipelines

Python connectors that land raw data to OneLake bronze layer. Located in `pipelines/`.

```
pipelines/
├── ingest_adp_api.py      # ADP Workforce Now API (OAuth2, paginated)
├── ingest_adp_csv.py      # ADP CSV exports via SFTP or S3
├── ingest_netsuite.py     # NetSuite SuiteAnalytics Connect (JDBC)
├── ingest_mysql.py        # Aurora MySQL — generic, config-driven (Jan-IT)
├── pipeline_template.py   # Shared orchestration: validate → land → log
├── utils.py               # Shared tools: config, landing, logging, alerting
├── config/                # One YAML per source (table list, queries, endpoints)
├── state/                 # Incremental watermarks — committed, updated on each run
│   └── pipeline_state.json
└── .env.example           # Copy to .env and fill in credentials — never commit .env
```

**Setup:**
```bash
cd pipelines
cp .env.example .env
# fill in .env with your credentials
```

**Run (ADP API — currently pointing to DummyJSON for testing):**
```bash
python pipelines/ingest_adp_api.py --dry-run   # first page only, nothing written
python pipelines/ingest_adp_api.py             # full run, lands to test_output/ or OneLake
```

**Switching from DummyJSON to real ADP:** update `ADP_BASE_URL` and auth vars in `.env`, swap table configs in `config/adp.yaml` to the real ADP blocks in the comments.

---

## Key Rules

- **Never commit `.env` or `profiles.yml`** — credentials stay local
- **One branch per session** — branch naming: `session/NN-description`
- **Run `dbt compile` before raising a PR** — must pass with zero errors
- **Check `PHASES.md` before building** — don't build Phase 03/04 models before Phase 02 is stable
- **`dim_site_assignment` is a stub** — do not populate until ADP scheduling adoption is confirmed
