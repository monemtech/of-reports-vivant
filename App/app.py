"""
OrderFloz — Management Reporting Dashboard
==========================================
Vivant Skin Care | Wholesale B2B Sales Intelligence

Features:
- Year-over-Year / period-over-period sales comparison by account
- Cin7 Contacts API for Sales Rep + Type enrichment
- HubSpot Tier enrichment
- Employee domain exclusion
- Parallel fetch with disk cache
- Filter, sort, chart, export
"""

# =============================================================================
# IMPORTS
# =============================================================================
import streamlit as st
import pandas as pd
import requests
import pickle
import hashlib
import json
import io
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================================================================
# PAGE CONFIG  (must be first Streamlit call)
# =============================================================================
st.set_page_config(
    page_title="OrderFloz Reports",
    page_icon="📊",
    layout="wide"
)

# =============================================================================
# CONSTANTS
# =============================================================================
CONFIG_FILE     = Path(".orderfloz_reports_config.json")
CACHE_META_FILE = Path(".orderfloz_cache_meta.json")
CACHE_DIR       = Path(".orderfloz_cache")
CACHE_DIR.mkdir(exist_ok=True)

HUBSPOT_TTL_HOURS = 4

BRANDING = {
    "company_name":  "OrderFloz",
    "primary_color": "#1a5276",
    "accent_color":  "#00d4aa",
}

CURRENT_YEAR = datetime.now().year

# Cin7 order fields — only what we use (cuts payload ~85%)
CIN7_ORDER_FIELDS = (
    "id,company,billingCompany,firstName,lastName,"
    "email,total,createdDate,modifiedDate,salesPersonId,source"
)

# =============================================================================
# CONFIG PERSISTENCE
# =============================================================================

def load_config() -> dict:
    try:
        if CONFIG_FILE.exists():
            return json.loads(CONFIG_FILE.read_text())
    except Exception:
        pass
    return {}


def save_config(data: dict):
    try:
        existing = load_config()
        existing.update(data)
        CONFIG_FILE.write_text(json.dumps(existing))
    except Exception:
        pass


def get_secret(key: str, default: str = "") -> str:
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


def get_excluded_domains() -> set:
    """
    Email domains whose orders are excluded (employees / internal accounts).
    Priority: saved config -> Streamlit secrets -> empty.
    """
    raw = load_config().get("excluded_domains", "") or get_secret("EXCLUDED_DOMAINS", "")
    if not raw:
        return set()
    return {d.strip().lower() for d in raw.split(",") if d.strip()}

# =============================================================================
# DISK CACHE
# =============================================================================

def _cache_key(label: str) -> str:
    return hashlib.md5(label.encode()).hexdigest()


def _load_cache_meta() -> dict:
    try:
        if CACHE_META_FILE.exists():
            return json.loads(CACHE_META_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_cache_meta(meta: dict):
    try:
        CACHE_META_FILE.write_text(json.dumps(meta))
    except Exception:
        pass


def cache_save_orders(label: str, orders: list, fingerprint: str):
    """Persist a period's orders. Never saves empty results (prevents cache poisoning)."""
    if not orders:
        return
    try:
        key  = _cache_key(label)
        path = CACHE_DIR / f"orders_{key}.pkl"
        with open(path, "wb") as f:
            pickle.dump(orders, f)
        meta = _load_cache_meta()
        meta[f"cin7_{key}"] = {
            "label":       label,
            "fingerprint": fingerprint,
            "saved_at":    datetime.now().isoformat(),
            "count":       len(orders),
        }
        _save_cache_meta(meta)
    except Exception:
        pass


def cache_load_orders(label: str, fingerprint: str):
    """
    Return cached orders if the fingerprint still matches, else None.
    Closed periods (fingerprint == "CLOSED") are always trusted.
    """
    try:
        key   = _cache_key(label)
        meta  = _load_cache_meta()
        entry = meta.get(f"cin7_{key}")
        if not entry or entry.get("count", 1) == 0:
            return None
        path = CACHE_DIR / f"orders_{key}.pkl"
        if not path.exists():
            return None
        if entry["fingerprint"] == fingerprint or fingerprint == "CLOSED":
            data = pickle.load(open(path, "rb"))
            return data if data else None
    except Exception:
        pass
    return None


def cache_save_hubspot(tiers: dict, owners: dict):
    try:
        path = CACHE_DIR / "hubspot.pkl"
        with open(path, "wb") as f:
            pickle.dump({"tiers": tiers, "owners": owners}, f)
        meta = _load_cache_meta()
        meta["hubspot"] = {"saved_at": datetime.now().isoformat()}
        _save_cache_meta(meta)
    except Exception:
        pass


def cache_load_hubspot():
    """Return (tiers, owners) if cache is within TTL, else (None, None)."""
    try:
        meta  = _load_cache_meta()
        entry = meta.get("hubspot")
        if not entry:
            return None, None
        age_h = (datetime.now() - datetime.fromisoformat(entry["saved_at"])).total_seconds() / 3600
        if age_h > HUBSPOT_TTL_HOURS:
            return None, None
        path = CACHE_DIR / "hubspot.pkl"
        if not path.exists():
            return None, None
        with open(path, "rb") as f:
            data = pickle.load(f)
        return data["tiers"], data["owners"]
    except Exception:
        pass
    return None, None


def cache_clear_all():
    try:
        for f in CACHE_DIR.iterdir():
            f.unlink()
        if CACHE_META_FILE.exists():
            CACHE_META_FILE.unlink()
    except Exception:
        pass


def cache_purge_empty_entries():
    """On startup, remove any zero-order cache entries from prior failed fetches."""
    try:
        meta  = _load_cache_meta()
        dirty = False
        for k, v in list(meta.items()):
            if k.startswith("cin7_") and v.get("count", 1) == 0:
                pkl = CACHE_DIR / f"orders_{k[5:]}.pkl"
                if pkl.exists():
                    pkl.unlink()
                del meta[k]
                dirty = True
        if dirty:
            _save_cache_meta(meta)
    except Exception:
        pass

# =============================================================================
# SESSION STATE INITIALISATION
# =============================================================================

def _init_session_state():
    defaults = {
        "report_data":             None,
        "cin7_orders_cache":       None,
        "hubspot_companies_cache": None,
        "audit":                   None,
        "periods":                 [],
        "config_loaded":           load_config(),
        "cin7_staff":              {},
        "cin7_customers":          {},
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

_init_session_state()
cache_purge_empty_entries()

# =============================================================================
# CIN7 -- CONNECTION TEST
# =============================================================================

def test_cin7_connection(username: str, api_key: str) -> tuple:
    try:
        r = requests.get(
            "https://api.cin7.com/api/v1/SalesOrders",
            auth=(username, api_key),
            params={"rows": 1},
            timeout=15,
        )
        if r.status_code == 200:   return True,  "Connected"
        if r.status_code == 401:   return False, "Invalid credentials"
        return False, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)

# =============================================================================
# CIN7 -- FINGERPRINT PROBE
# =============================================================================

def probe_cin7_fingerprint(username: str, api_key: str,
                            start_date: str, end_date: str) -> str:
    """
    Fetch the single most-recently-modified order in a date range.
    Returns "id:modifiedDate" as a lightweight change fingerprint.
    Fast -- one API call, one row.
    """
    try:
        r = requests.get(
            "https://api.cin7.com/api/v1/SalesOrders",
            auth=(username, api_key),
            params={
                "where":  f"createdDate >= '{start_date}' AND createdDate <= '{end_date}'",
                "rows":   1,
                "order":  "modifiedDate desc",
                "fields": "id,modifiedDate",
            },
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            if data:
                o = data[0]
                return f"{o.get('id', '')}:{o.get('modifiedDate', '')}"
    except Exception:
        pass
    return ""

# =============================================================================
# CIN7 -- ORDERS FETCH
# =============================================================================

def fetch_orders_by_date_range(username: str, api_key: str,
                                start_date: str, end_date: str,
                                label: str = "") -> list:
    """Paginate through Cin7 SalesOrders for a date range."""
    all_orders = []
    page       = 1
    use_fields = True   # try slim fetch first; fall back if server rejects

    while True:
        params = {
            "where": f"createdDate >= '{start_date}' AND createdDate <= '{end_date}'",
            "page":  page,
            "rows":  250,
        }
        if use_fields:
            params["fields"] = CIN7_ORDER_FIELDS

        try:
            r = requests.get(
                "https://api.cin7.com/api/v1/SalesOrders",
                auth=(username, api_key),
                params=params,
                timeout=60,
            )

            if r.status_code in (400, 422) and use_fields:
                use_fields = False
                continue

            if r.status_code != 200:
                st.warning(f"Cin7 {r.status_code} on {label} page {page}: {r.text[:200]}")
                break

            orders = r.json()
            if not orders:
                break
            all_orders.extend(orders)
            if len(orders) < 250:
                break
            page += 1

        except Exception as e:
            st.warning(f"Fetch error {label} page {page}: {e}")
            break

    return all_orders

# =============================================================================
# CIN7 -- STAFF (id -> name mapping)
# =============================================================================

def fetch_cin7_staff(username: str, api_key: str) -> dict:
    """
    Return {user_id: "First Last"} for all Cin7 users.
    Session-cached -- only fetched once per browser session.
    """
    if st.session_state.cin7_staff:
        return st.session_state.cin7_staff

    staff = {}
    try:
        r = requests.get(
            "https://api.cin7.com/api/v1/Users",
            auth=(username, api_key),
            timeout=15,
        )
        if r.status_code == 200:
            for u in r.json():
                uid  = u.get("id") or u.get("userId")
                name = f"{u.get('firstName','').strip()} {u.get('lastName','').strip()}".strip()
                if uid and name:
                    staff[uid] = name
    except Exception:
        pass

    st.session_state.cin7_staff = staff
    return staff

# =============================================================================
# CIN7 -- CONTACTS (company -> rep + type)
# =============================================================================

def fetch_cin7_customers(username: str, api_key: str) -> dict:
    """
    Fetch all Cin7 Contacts and return:
      { "COMPANY NAME UPPER": { "rep": "First Last", "type": "10%" } }

    Sales Rep  <- salesRepresentative field on contact
    Type       <- customFields["Members_1037"]  (Cin7-confirmed key for the "Type" field)
                 Falls back to any key named "type" (case-insensitive).

    Session-cached -- only fetched once per Generate Report click.
    """
    if st.session_state.cin7_customers:
        return st.session_state.cin7_customers

    customers = {}
    page      = 1
    fields    = "id,name,salesRepresentative,customFields"

    while True:
        try:
            r = requests.get(
                "https://api.cin7.com/api/v1/Contacts",
                auth=(username, api_key),
                params={"page": page, "rows": 250, "fields": fields},
                timeout=30,
            )
            if r.status_code != 200:
                break

            data = r.json()
            if not data:
                break

            for c in data:
                name = (c.get("name") or "").strip().upper()
                if not name:
                    continue

                rep   = (c.get("salesRepresentative") or "").strip()
                cf    = c.get("customFields") or {}
                ctype = ""

                # Look for Cin7-confirmed field key first, then generic "type" fallback
                for k, v in cf.items():
                    if k == "Members_1037" or k.lower() == "type":
                        ctype = str(v).strip() if v else ""
                        break

                customers[name] = {"rep": rep, "type": ctype}

            if len(data) < 250:
                break
            page += 1

        except Exception:
            break

    st.session_state.cin7_customers = customers
    return customers

# =============================================================================
# HUBSPOT -- CONNECTION TEST
# =============================================================================

def test_hubspot_connection(api_key: str) -> tuple:
    try:
        r = requests.get(
            "https://api.hubapi.com/crm/v3/objects/companies",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"limit": 1},
            timeout=15,
        )
        if r.status_code == 200:  return True,  "Connected"
        if r.status_code == 401:  return False, "Invalid API key"
        return False, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)

# =============================================================================
# HUBSPOT -- COMPANIES + OWNERS
# =============================================================================

def fetch_hubspot_company_data(api_key: str, tier_property: str = "commission_tier") -> tuple:
    """
    Single paginated fetch that returns BOTH tier and owner data.
    Returns: (tiers_dict, owners_dict)
      tiers_dict:  {COMPANY_UPPER: tier_string}
      owners_dict: {COMPANY_UPPER: hubspot_owner_id}
    """
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    tiers   = {}
    owners  = {}
    after   = None

    while True:
        params = {
            "limit":      100,
            "properties": f"name,{tier_property},hubspot_owner_id",
        }
        if after:
            params["after"] = after

        try:
            r = requests.get(
                "https://api.hubapi.com/crm/v3/objects/companies",
                headers=headers,
                params=params,
                timeout=30,
            )
            if r.status_code != 200:
                break

            data = r.json()
            for company in data.get("results", []):
                props = company.get("properties", {})
                name  = (props.get("name") or "").strip().upper()
                if not name:
                    continue
                tiers[name]  = props.get(tier_property, "") or ""
                owner_id     = props.get("hubspot_owner_id") or ""
                if owner_id:
                    owners[name] = str(owner_id)

            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break

        except Exception as e:
            st.warning(f"HubSpot fetch error: {e}")
            break

    return tiers, owners


def fetch_hubspot_owners(api_key: str) -> dict:
    """Return {owner_id_string: "Full Name"} for all HubSpot owners."""
    try:
        r = requests.get(
            "https://api.hubapi.com/crm/v3/owners",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"limit": 100},
            timeout=15,
        )
        if r.status_code != 200:
            return {}
        result = {}
        for o in r.json().get("results", []):
            oid  = str(o.get("id", ""))
            name = f"{o.get('firstName','').strip()} {o.get('lastName','').strip()}".strip() \
                   or o.get("email", "")
            if oid:
                result[oid] = name
        return result
    except Exception:
        return {}

# =============================================================================
# ORDER AGGREGATION
# =============================================================================

def aggregate_orders_by_company(orders: list, periods: list,
                                  cin7_staff: dict = None) -> tuple:
    """
    Aggregate flat Cin7 order list into per-company sales totals per period.
    Excludes:
      * Shopify / Retail channel orders (B2C)
      * Orders where customer email matches an excluded domain
    Returns: (company_data dict, audit dict)
    """
    excluded_domains = get_excluded_domains()
    period_labels    = [p["label"] for p in periods]

    def get_period_label(date_str):
        if not date_str:
            return None
        try:
            d = datetime.fromisoformat(date_str[:10]).date()
        except Exception:
            return None
        for p in periods:
            if p["start"] <= d <= p["end"]:
                return p["label"]
        return None

    def email_domain(email: str) -> str:
        email = (email or "").strip().lower()
        return email.split("@")[-1] if "@" in email else ""

    company_data = {}
    audit = {
        "total_raw":              len(orders),
        "included":               0,
        "excluded_source":        0,
        "excluded_domain":        0,
        "excluded_no_period":     0,
        "excluded_zero_total":    0,
        "unknown_company":        0,
        "unique_companies":       0,
        "by_period":              {lbl: {"included": 0, "excluded_source": 0, "revenue": 0.0}
                                   for lbl in period_labels},
        "excluded_sources":       {},
        "excluded_domain_counts": {},
        "sample_excluded":        [],
    }

    for order in orders:
        company = (order.get("company") or order.get("billingCompany") or "").strip()
        if not company:
            first   = order.get("firstName", "")
            last    = order.get("lastName",  "")
            company = f"{first} {last}".strip() or "Unknown"
            if company == "Unknown":
                audit["unknown_company"] += 1

        total        = float(order.get("total") or 0)
        sp_id        = order.get("salesPersonId")
        rep_name     = (cin7_staff or {}).get(sp_id, "") if sp_id else ""
        cust_email   = (order.get("email") or "").strip()
        created_date = order.get("createdDate", "")
        source       = (order.get("source") or "").strip()
        source_lower = source.lower()
        period_label = get_period_label(created_date)

        # Exclude: Shopify / retail B2C
        if "shopify" in source_lower or "retail" in source_lower:
            audit["excluded_source"] += 1
            audit["excluded_sources"][source] = audit["excluded_sources"].get(source, 0) + 1
            if period_label in period_labels:
                audit["by_period"][period_label]["excluded_source"] += 1
            if len(audit["sample_excluded"]) < 10:
                audit["sample_excluded"].append({
                    "reason":  f"source={source}",
                    "company": company,
                    "total":   total,
                    "date":    created_date[:10] if created_date else "",
                })
            continue

        # Exclude: employee email domains
        domain = email_domain(cust_email)
        if excluded_domains and domain in excluded_domains:
            audit["excluded_domain"] += 1
            audit["excluded_domain_counts"][domain] = \
                audit["excluded_domain_counts"].get(domain, 0) + 1
            if len(audit["sample_excluded"]) < 10:
                audit["sample_excluded"].append({
                    "reason":  f"excluded domain ({domain})",
                    "company": company,
                    "email":   cust_email,
                    "total":   total,
                    "date":    created_date[:10] if created_date else "",
                })
            continue

        # Exclude: outside selected date windows
        if period_label is None or period_label not in period_labels:
            audit["excluded_no_period"] += 1
            continue

        if total == 0:
            audit["excluded_zero_total"] += 1

        if company not in company_data:
            company_data[company] = {"rep": rep_name, "order_count": 0}
            for lbl in period_labels:
                company_data[company][lbl] = 0.0

        company_data[company][period_label]  += total
        company_data[company]["order_count"] += 1
        if rep_name and not company_data[company]["rep"]:
            company_data[company]["rep"] = rep_name

        audit["included"]                                  += 1
        audit["by_period"][period_label]["included"]       += 1
        audit["by_period"][period_label]["revenue"]        += total

    audit["unique_companies"] = len(company_data)
    return company_data, audit

# =============================================================================
# REPORT DATAFRAME BUILDER
# =============================================================================

def build_report_dataframe(company_data: dict,
                            hubspot_tiers: dict,
                            periods: list,
                            company_owners: dict = None,
                            owners_lookup: dict  = None,
                            cin7_customers: dict = None) -> pd.DataFrame:
    """
    Build the main report DataFrame.
    Rep and Type come from Cin7 Contacts (authoritative).
    Tier comes from HubSpot.
    """
    labels        = [p["label"] for p in periods]
    primary_label = labels[-1]
    comp_label    = labels[-2] if len(labels) >= 2 else None

    primary_col = primary_label
    comp_col    = comp_label if comp_label else "Comparison"

    rows = []
    for company, data in company_data.items():
        primary_sales = data.get(primary_label, 0)
        comp_sales    = data.get(comp_label, 0) if comp_label else 0

        if primary_sales == 0 and comp_sales == 0:
            continue

        # Tier from HubSpot
        tier = hubspot_tiers.get(company.upper(), "")

        # Rep + Type from Cin7 Contacts; HubSpot owner as rep fallback
        cin7_cust = (cin7_customers or {}).get(company.upper(), {})
        rep       = cin7_cust.get("rep", "") or data.get("rep", "")
        if not rep and company_owners and owners_lookup:
            owner_id = company_owners.get(company.upper(), "")
            rep      = owners_lookup.get(owner_id, "") if owner_id else ""
        ctype = cin7_cust.get("type", "")

        if comp_sales > 0:
            change_pct = ((primary_sales - comp_sales) / comp_sales) * 100
        elif primary_sales > 0:
            change_pct = 100.0
        else:
            change_pct = 0.0

        rows.append({
            "Account":         company,
            primary_col:       primary_sales,
            comp_col:          comp_sales,
            "$ Change":        primary_sales - comp_sales,
            "% Change":        change_pct,
            "Type":            ctype,
            "Tier":            tier,
            "Sales Rep":       rep,
            "_order_count":    data.get("order_count", 0),
            "_primary_col":    primary_col,
            "_comparison_col": comp_col,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(primary_col, ascending=False).reset_index(drop=True)
    return df

# =============================================================================
# EXPORT
# =============================================================================

def export_to_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Sales Report", index=False)
        ws = writer.sheets["Sales Report"]
        for idx, col in enumerate(df.columns):
            width = min(max(df[col].astype(str).map(len).max(), len(col)) + 2, 50)
            ws.column_dimensions[chr(65 + idx)].width = width
    return output.getvalue()

# =============================================================================
# CSS
# =============================================================================

def inject_css():
    st.markdown(f"""
    <style>
        .main-header {{
            text-align: center;
            padding: 1rem 0;
            margin-bottom: 1.5rem;
        }}
        .main-header h1 {{
            color: {BRANDING['primary_color']};
            margin: 0;
        }}
    </style>
    """, unsafe_allow_html=True)

# =============================================================================
# CHARTS
# =============================================================================

def create_yoy_chart(df: pd.DataFrame):
    import plotly.graph_objects as go
    pc  = df["_primary_col"].iloc[0]
    cc  = df["_comparison_col"].iloc[0]
    top = df.nlargest(15, pc)
    fig = go.Figure()
    if cc in df.columns:
        fig.add_trace(go.Bar(name=cc, x=top["Account"], y=top[cc], marker_color="#3498db"))
    fig.add_trace(go.Bar(name=pc, x=top["Account"], y=top[pc], marker_color="#00d4aa"))
    fig.update_layout(
        title=f"Top 15 Accounts - {pc} vs {cc}",
        barmode="group", xaxis_tickangle=-45, height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def create_rep_chart(df: pd.DataFrame):
    import plotly.express as px
    pc       = df["_primary_col"].iloc[0]
    rep_data = df.groupby("Sales Rep").agg(Sales=(pc, "sum"),
                                           Accounts=("Account", "count")).reset_index()
    rep_data = rep_data[rep_data["Sales Rep"] != ""].sort_values("Sales", ascending=True)
    if rep_data.empty:
        return None
    return px.bar(rep_data, y="Sales Rep", x="Sales", orientation="h",
                  title=f"{pc} by Sales Rep", color="Sales",
                  color_continuous_scale="Blues", height=400)


def create_tier_chart(df: pd.DataFrame):
    import plotly.express as px
    pc        = df["_primary_col"].iloc[0]
    tier_data = df.groupby("Tier").agg(Sales=(pc, "sum")).reset_index()
    tier_data = tier_data[tier_data["Tier"] != ""]
    if tier_data.empty:
        return None
    return px.pie(tier_data, values="Sales", names="Tier",
                  title=f"{pc} by Tier",
                  color_discrete_sequence=px.colors.qualitative.Set2, height=400)


def create_scatter_chart(df: pd.DataFrame):
    import plotly.express as px
    pc = df["_primary_col"].iloc[0]
    cc = df["_comparison_col"].iloc[0]
    if cc not in df.columns:
        return None
    plot_df = df[(df[pc] > 0) | (df[cc] > 0)].copy()
    if plot_df.empty:
        return None
    fig = px.scatter(
        plot_df, x=cc, y=pc, size=pc,
        color="Tier" if plot_df["Tier"].any() else None,
        hover_name="Account", hover_data={"$ Change": True, "% Change": True},
        title=f"{pc} vs {cc}", height=500,
    )
    mx = max(plot_df[cc].max(), plot_df[pc].max())
    fig.add_shape(type="line", x0=0, y0=0, x1=mx, y1=mx,
                  line=dict(color="gray", dash="dash"))
    return fig

# =============================================================================
# PERIOD RESOLUTION HELPERS
# =============================================================================

def _quarter_bounds(year: int, q: int):
    starts = {1: (1, 1),  2: (4, 1),  3: (7, 1),  4: (10, 1)}
    ends   = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
    s, e   = starts[q], ends[q]
    return datetime(year, s[0], s[1]).date(), datetime(year, e[0], e[1]).date()


def resolve_primary_period(period_name: str, today, cy: int, cm: int, cq: int,
                            custom_start=None, custom_end=None):
    """Returns (label, start_date, end_date)."""
    import calendar

    if period_name == "This Month":
        last_day = calendar.monthrange(cy, cm)[1]
        return (datetime(cy, cm, 1).strftime("%b %Y"),
                datetime(cy, cm, 1).date(),
                min(datetime(cy, cm, last_day).date(), today))

    if period_name == "Last Month":
        lm = cm - 1 if cm > 1 else 12
        ly = cy if cm > 1 else cy - 1
        last_day = calendar.monthrange(ly, lm)[1]
        return (datetime(ly, lm, 1).strftime("%b %Y"),
                datetime(ly, lm, 1).date(),
                datetime(ly, lm, last_day).date())

    if period_name == "This Quarter":
        s, e = _quarter_bounds(cy, cq)
        return (f"Q{cq} {cy}", s, min(e, today))

    if period_name == "Last Quarter":
        lq = cq - 1 if cq > 1 else 4
        ly = cy if cq > 1 else cy - 1
        s, e = _quarter_bounds(ly, lq)
        return (f"Q{lq} {ly}", s, e)

    if period_name == "Year to Date":
        return (f"{cy} YTD", datetime(cy, 1, 1).date(), today)

    if period_name == "Last 12 Months":
        return ("Last 12 Months", today.replace(year=today.year - 1), today)

    if period_name == "This Year (Full)":
        return (str(cy), datetime(cy, 1, 1).date(), datetime(cy, 12, 31).date())

    if period_name == "Last Year (Full)":
        return (str(cy - 1), datetime(cy-1, 1, 1).date(), datetime(cy-1, 12, 31).date())

    if period_name == "Last 30 Days":
        return ("Last 30 Days", today - timedelta(days=30), today)

    if period_name == "Last 60 Days":
        return ("Last 60 Days", today - timedelta(days=60), today)

    if period_name == "Last 90 Days":
        return ("Last 90 Days", today - timedelta(days=90), today)

    if period_name == "Custom Range":
        cs, ce = custom_start or today, custom_end or today
        label  = f"{cs.strftime('%b %d')} - {ce.strftime('%b %d, %Y')}"
        return (label, cs, ce)

    # Fallback
    return (f"{cy} YTD", datetime(cy, 1, 1).date(), today)


def same_period_prior_year(start, end):
    try:
        cs = start.replace(year=start.year - 1)
    except ValueError:
        cs = start - timedelta(days=365)
    try:
        ce = end.replace(year=end.year - 1)
    except ValueError:
        ce = end - timedelta(days=365)
    return cs, ce

# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    inject_css()

    st.markdown(f"""
    <div class="main-header">
        <h1>📊 {BRANDING['company_name']} Management Reports</h1>
        <p style="color:#666;">Sales Intelligence Dashboard</p>
    </div>
    """, unsafe_allow_html=True)

    # =========================================================================
    # SIDEBAR
    # =========================================================================
    with st.sidebar:
        st.header("⚙️ Data Sources")

        # Cin7 credentials
        st.subheader("📦 Cin7 API")
        cin7_user = st.text_input("Username", value=get_secret("CIN7_USERNAME"),
                                   key="cin7_user")
        cin7_key  = st.text_input("API Key",  value=get_secret("CIN7_API_KEY"),
                                   type="password", key="cin7_key")

        # Defined unconditionally right after the inputs -- never inside any if/else
        can_generate = bool(cin7_user and cin7_key)

        if cin7_user and cin7_key:
            if st.button("Test Cin7", key="test_cin7"):
                ok, msg = test_cin7_connection(cin7_user, cin7_key)
                (st.success if ok else st.error)(f"{'OK' if ok else 'ERR'}: {msg}")

        st.divider()

        # HubSpot credentials
        st.subheader("🟠 HubSpot API")
        hubspot_key   = st.text_input("Private App Token",
                                       value=get_secret("HUBSPOT_API_KEY"),
                                       type="password", key="hubspot_key")
        tier_property = st.text_input("Tier Property Name",
                                       value=get_secret("HUBSPOT_TIER_PROPERTY", "commission_tier"),
                                       key="tier_prop",
                                       help="Internal HubSpot property name for commission tier")

        if hubspot_key:
            if st.button("Test HubSpot", key="test_hs"):
                ok, msg = test_hubspot_connection(hubspot_key)
                (st.success if ok else st.error)(f"{'OK' if ok else 'ERR'}: {msg}")

        st.divider()

        # Period selection
        st.subheader("📅 Report Period")
        today = datetime.now().date()
        cy    = today.year
        cm    = today.month
        cq    = (cm - 1) // 3 + 1

        PERIOD_OPTIONS = [
            "This Month", "Last Month",
            "This Quarter", "Last Quarter",
            "Year to Date", "Last 12 Months",
            "This Year (Full)", "Last Year (Full)",
            "Last 30 Days", "Last 60 Days", "Last 90 Days",
            "Custom Range",
        ]

        cfg          = st.session_state.config_loaded
        saved_period = cfg.get("last_period", "Year to Date")
        period_idx   = PERIOD_OPTIONS.index(saved_period) if saved_period in PERIOD_OPTIONS else 4

        selected_period = st.selectbox("Primary Period", PERIOD_OPTIONS,
                                        index=period_idx, key="primary_period")

        custom_start, custom_end = None, None
        if selected_period == "Custom Range":
            custom_start = st.date_input("From", value=datetime(cy, 1, 1).date(),
                                          key="custom_start")
            custom_end   = st.date_input("To",   value=today, key="custom_end")

        p_label, p_start, p_end = resolve_primary_period(
            selected_period, today, cy, cm, cq, custom_start, custom_end)

        # Compare Against
        COMPARE_OPTIONS = [
            "Same Period Last Year",
            "Previous Period",
            "Custom Comparison Range",
            "None",
        ]
        saved_compare = cfg.get("last_compare", "Same Period Last Year")
        compare_idx   = COMPARE_OPTIONS.index(saved_compare) \
                        if saved_compare in COMPARE_OPTIONS else 0

        compare_to = st.selectbox("Compare Against", COMPARE_OPTIONS,
                                   index=compare_idx, key="compare_to")

        comp_custom_start, comp_custom_end = None, None
        if compare_to == "Custom Comparison Range":
            try:
                default_cs = p_start.replace(year=p_start.year - 1)
            except ValueError:
                default_cs = p_start - timedelta(days=365)
            try:
                default_ce = p_end.replace(year=p_end.year - 1)
            except ValueError:
                default_ce = p_end - timedelta(days=365)
            comp_custom_start = st.date_input("Compare From", value=default_cs,
                                               key="comp_cstart")
            comp_custom_end   = st.date_input("Compare To",   value=default_ce,
                                               key="comp_cend")

        # Resolve comparison period
        comp = None
        if compare_to == "Same Period Last Year":
            cs, ce  = same_period_prior_year(p_start, p_end)
            c_label = p_label.replace(str(p_start.year), str(cs.year)) \
                      if str(p_start.year) in p_label else f"{p_label} (Prior Year)"
            comp = (c_label, cs, ce)
        elif compare_to == "Previous Period":
            delta = p_end - p_start
            ce    = p_start - timedelta(days=1)
            cs    = ce - delta
            comp  = (f"{cs.strftime('%b %d')} - {ce.strftime('%b %d, %Y')}", cs, ce)
        elif compare_to == "Custom Comparison Range" and comp_custom_start and comp_custom_end:
            c_label = f"{comp_custom_start.strftime('%b %d')} - {comp_custom_end.strftime('%b %d, %Y')}"
            comp    = (c_label, comp_custom_start, comp_custom_end)

        # Build periods list (comparison first, primary last)
        if comp:
            periods = [
                {"label": comp[0], "start": comp[1], "end": comp[2]},
                {"label": p_label, "start": p_start, "end": p_end},
            ]
        else:
            periods = [
                {"label": p_label, "start": p_start, "end": p_end},
            ]

        # Resolved dates preview
        st.caption(f"📅 **{p_label}:** {p_start.strftime('%b %d, %Y')} to {p_end.strftime('%b %d, %Y')}")
        if comp:
            p_days = (p_end   - p_start).days + 1
            c_days = (comp[2] - comp[1]).days + 1
            st.caption(
                f"📅 **vs. {comp[0]}:** "
                f"{comp[1].strftime('%b %d, %Y')} to {comp[2].strftime('%b %d, %Y')}"
            )
            icon = "OK" if p_days == c_days else "NOTE"
            st.caption(f"{icon}: {p_days}d vs {c_days}d")

        st.divider()

        # Excluded Domains
        st.subheader("🚫 Excluded Domains")
        st.caption("Orders from these email domains are excluded (employees / internal).")
        _exc_raw = load_config().get(
            "excluded_domains",
            get_secret("EXCLUDED_DOMAINS", "vivantskincare.com")
        )
        excluded_input = st.text_area(
            "One domain per line",
            value="\n".join(d.strip() for d in _exc_raw.split(",") if d.strip()),
            height=80,
            key="excluded_domains_input",
        )
        if st.button("💾 Save Exclusions", use_container_width=True):
            domains_csv = ", ".join(
                d.strip().lower() for d in excluded_input.splitlines() if d.strip())
            save_config({"excluded_domains": domains_csv})
            cache_clear_all()
            st.session_state.report_data = None
            st.success("Saved. Regenerate report to apply.")
            st.rerun()

        # Cache status
        meta           = _load_cache_meta()
        cached_periods = [v["label"] for k, v in meta.items() if k.startswith("cin7_")]
        hs_entry       = meta.get("hubspot")
        if cached_periods:
            hs_age_str = ""
            if hs_entry:
                hs_h = (datetime.now() - datetime.fromisoformat(
                    hs_entry["saved_at"])).total_seconds() / 3600
                hs_age_str = f" | HubSpot {hs_h:.1f}h ago"
            st.caption(f"💾 Cache: {len(cached_periods)} period(s){hs_age_str}")
            if st.button("🗑️ Clear Cache", use_container_width=True):
                cache_clear_all()
                st.session_state.report_data = None
                st.rerun()

        st.divider()

        # Generate Report button
        if st.button("🔄 Generate Report", type="primary",
                     use_container_width=True, disabled=not can_generate):

            with st.spinner("Fetching data..."):
                progress  = st.empty()
                save_config({"last_period": selected_period, "last_compare": compare_to})
                st.session_state.periods = periods

                today_dt   = datetime.now().date()
                all_orders = []
                cache_hits = 0
                misses     = 0

                # Step 1: probe all periods in parallel
                progress.text("Checking for updates...")

                def _probe(p, user, key):
                    s  = p["start"].strftime("%Y-%m-%dT00:00:00Z")
                    e  = p["end"].strftime("%Y-%m-%dT23:59:59Z")
                    fp = "CLOSED" if p["end"] < today_dt else \
                         probe_cin7_fingerprint(user, key, s, e)
                    return p, s, e, fp

                probed = []
                with ThreadPoolExecutor(max_workers=4) as ex:
                    futs = [ex.submit(_probe, p, cin7_user, cin7_key) for p in periods]
                    for f in as_completed(futs):
                        probed.append(f.result())

                # Step 2: cache check
                needs_fetch = []
                for p, s, e, fp in probed:
                    cached = cache_load_orders(p["label"], fp)
                    if cached is not None:
                        all_orders.extend(cached)
                        cache_hits += 1
                    else:
                        misses += 1
                        needs_fetch.append((p, s, e, fp))

                # Step 3: parallel full fetch + HubSpot + Contacts
                hs_result = [None, None, None]  # [tiers, owners_by_company, owners_lookup]

                def _full_fetch(p, s, e, fp, user, key):
                    orders = fetch_orders_by_date_range(user, key, s, e, label=p["label"])
                    cache_save_orders(p["label"], orders, fp)
                    return p["label"], orders

                def _fetch_hubspot(api_key, tier_prop):
                    tiers, owners = cache_load_hubspot()
                    if tiers is not None:
                        lookup = fetch_hubspot_owners(api_key)
                        return tiers, owners, lookup
                    tiers, owners = fetch_hubspot_company_data(api_key, tier_prop)
                    lookup = fetch_hubspot_owners(api_key)
                    cache_save_hubspot(tiers, owners)
                    return tiers, owners, lookup

                def _fetch_customers(user, key):
                    # Always reset so contacts are re-fetched on each Generate click
                    st.session_state.cin7_customers = {}
                    return fetch_cin7_customers(user, key)

                tasks = {}
                with ThreadPoolExecutor(max_workers=6) as ex:
                    for args in needs_fetch:
                        fut = ex.submit(_full_fetch, *args, cin7_user, cin7_key)
                        tasks[fut] = "cin7"
                    if hubspot_key:
                        hsfut = ex.submit(_fetch_hubspot, hubspot_key, tier_property)
                        tasks[hsfut] = "hubspot"
                    custfut = ex.submit(_fetch_customers, cin7_user, cin7_key)
                    tasks[custfut] = "customers"

                    for fut in as_completed(tasks):
                        kind = tasks[fut]
                        try:
                            if kind == "cin7":
                                lbl, orders = fut.result()
                                all_orders.extend(orders)
                                progress.text(f"Downloaded {lbl}")
                            elif kind == "hubspot":
                                tiers, owners, lookup = fut.result()
                                hs_result = [tiers, owners, lookup]
                            elif kind == "customers":
                                st.session_state.cin7_customers = fut.result()
                        except Exception as ex_err:
                            st.warning(f"Task error ({kind}): {ex_err}")

                hubspot_tiers  = hs_result[0] or {}
                company_owners = hs_result[1] or {}
                owners_lookup  = hs_result[2] or {}

                # Build report
                progress.text("Building report...")
                cin7_staff   = fetch_cin7_staff(cin7_user, cin7_key)
                company_data, audit = aggregate_orders_by_company(
                    all_orders, periods, cin7_staff=cin7_staff)
                df = build_report_dataframe(
                    company_data, hubspot_tiers, periods,
                    company_owners, owners_lookup,
                    cin7_customers=st.session_state.cin7_customers)

                st.session_state.report_data             = df
                st.session_state.cin7_orders_cache       = all_orders
                st.session_state.hubspot_companies_cache = hubspot_tiers
                st.session_state.audit                   = audit

                progress.text(
                    f"Done. {cache_hits} period(s) from cache, {misses} refreshed.")
                st.rerun()

    # =========================================================================
    # MAIN CONTENT AREA
    # =========================================================================
    df = st.session_state.report_data

    # Guard: discard stale session data missing required structural columns
    REQUIRED = {"Account", "$ Change", "% Change", "Tier", "Sales Rep",
                "_primary_col", "_comparison_col"}
    if df is not None and not REQUIRED.issubset(df.columns):
        st.session_state.report_data = None
        df = None

    if df is None:
        st.info("👈 Configure your API credentials and click **Generate Report** to begin.")
        st.markdown("""
        | Column | Description |
        |---|---|
        | **Account** | Company / account name |
        | **YTD Sales** | Revenue for the selected primary period |
        | **Prior Year** | Revenue for the comparison period |
        | **$ Change** | Dollar difference |
        | **% Change** | Growth / decline % |
        | **Type** | Account type from Cin7 (6%, 10%, HA) |
        | **Tier** | Commission tier from HubSpot |
        | **Sales Rep** | Assigned rep from Cin7 Contacts |
        """)
        return

    primary_col = df["_primary_col"].iloc[0]
    comp_col    = df["_comparison_col"].iloc[0]
    periods     = st.session_state.get("periods", [])

    # Filters
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        reps         = ["All"] + sorted([r for r in df["Sales Rep"].unique() if r])
        selected_rep = st.selectbox("Sales Rep", reps)
    with fc2:
        tiers_list   = sorted([t for t in df["Tier"].unique() if t])
        has_no_tier  = df["Tier"].eq("").any() or df["Tier"].isna().any()
        tier_options = ["All"] + tiers_list + (["(No Tier)"] if has_no_tier else [])
        selected_tier = st.selectbox("Tier", tier_options)
    with fc3:
        types_list   = sorted([t for t in df["Type"].unique() if t]) \
                       if "Type" in df.columns else []
        has_no_type  = (df["Type"].eq("").any() or df["Type"].isna().any()) \
                       if "Type" in df.columns else False
        type_options  = ["All"] + types_list + (["(No Type)"] if has_no_type else [])
        selected_type = st.selectbox("Type", type_options)
    with fc4:
        min_sales = st.number_input(f"Min {primary_col} ($)", min_value=0, value=0, step=500)
    with fc5:
        sort_options = [
            f"{primary_col} down", f"{primary_col} up",
            f"{comp_col} down",    f"{comp_col} up",
            "$ Change down", "$ Change up",
            "% Change down", "% Change up",
            "Account A-Z",
        ]
        sort_by = st.selectbox("Sort By", sort_options)

    # Apply filters
    fdf = df.copy()
    if selected_rep != "All":
        fdf = fdf[fdf["Sales Rep"] == selected_rep]
    if selected_tier == "(No Tier)":
        fdf = fdf[fdf["Tier"].eq("") | fdf["Tier"].isna()]
    elif selected_tier != "All":
        fdf = fdf[fdf["Tier"] == selected_tier]
    if "Type" in fdf.columns:
        if selected_type == "(No Type)":
            fdf = fdf[fdf["Type"].eq("") | fdf["Type"].isna()]
        elif selected_type != "All":
            fdf = fdf[fdf["Type"] == selected_type]
    if min_sales > 0:
        fdf = fdf[fdf[primary_col] >= min_sales]

    sort_map = {
        f"{primary_col} down": (primary_col, False),
        f"{primary_col} up":   (primary_col, True),
        f"{comp_col} down":    (comp_col,    False),
        f"{comp_col} up":      (comp_col,    True),
        "$ Change down":       ("$ Change",  False),
        "$ Change up":         ("$ Change",  True),
        "% Change down":       ("% Change",  False),
        "% Change up":         ("% Change",  True),
        "Account A-Z":         ("Account",   True),
    }
    scol, sasc = sort_map.get(sort_by, (primary_col, False))
    if scol in fdf.columns:
        fdf = fdf.sort_values(scol, ascending=sasc).reset_index(drop=True)

    # Summary metrics
    st.divider()
    total_primary = fdf[primary_col].sum()
    total_comp    = fdf[comp_col].sum() if comp_col in fdf.columns else 0
    total_chg_d   = fdf["$ Change"].sum()
    total_chg_p   = ((total_primary - total_comp) / total_comp * 100) \
                    if total_comp > 0 else 0
    growing   = (fdf["% Change"] > 0).sum()
    declining = (fdf["% Change"] < 0).sum()

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    with m1: st.metric("Accounts",  f"{len(fdf):,}")
    with m2: st.metric(primary_col, f"${total_primary:,.0f}")
    with m3: st.metric(comp_col,    f"${total_comp:,.0f}")
    with m4: st.metric("$ Change",  f"${total_chg_d:+,.0f}")
    with m5: st.metric("% Change",  f"{total_chg_p:+.1f}%")
    with m6: st.metric("Up / Down", f"{growing} / {declining}")

    st.divider()

    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📋 Account Report", "📈 Charts", "📤 Export", "🔍 Data Audit"])

    # TAB 1: Account Report
    with tab1:
        if len(periods) == 2:
            pri, cmp = periods[1], periods[0]
            st.markdown(
                f"**{pri['label']}**  "
                f"{pri['start'].strftime('%b %d, %Y')} to {pri['end'].strftime('%b %d, %Y')}"
                f"  |  "
                f"**vs. {cmp['label']}**  "
                f"{cmp['start'].strftime('%b %d, %Y')} to {cmp['end'].strftime('%b %d, %Y')}"
            )
        elif len(periods) == 1:
            pri = periods[0]
            st.markdown(
                f"**{pri['label']}**  "
                f"{pri['start'].strftime('%b %d, %Y')} to {pri['end'].strftime('%b %d, %Y')}"
            )
        st.caption(f"{len(fdf)} accounts")

        display_cols = ["Account", primary_col, comp_col,
                        "$ Change", "% Change", "Type", "Tier", "Sales Rep"]
        display_cols = [c for c in display_cols if c in fdf.columns]
        display_df   = fdf[display_cols].copy()

        col_cfg = {
            "Account":   st.column_config.TextColumn("Account",    width="medium"),
            primary_col: st.column_config.NumberColumn(primary_col, format="$%.2f", width="small"),
            "$ Change":  st.column_config.NumberColumn("$ Change",  format="$%.2f", width="small"),
            "% Change":  st.column_config.NumberColumn("% Change",  format="%.1f%%", width="small"),
            "Type":      st.column_config.TextColumn("Type",        width="small"),
            "Tier":      st.column_config.TextColumn("Tier",        width="small"),
            "Sales Rep": st.column_config.TextColumn("Sales Rep",   width="small"),
        }
        if comp_col in display_df.columns:
            col_cfg[comp_col] = st.column_config.NumberColumn(
                comp_col, format="$%.2f", width="small")

        st.dataframe(display_df, use_container_width=True, hide_index=True,
                     column_config=col_cfg,
                     height=min(600, (len(display_df) + 1) * 35 + 38))

    # TAB 2: Charts
    with tab2:
        st.subheader("📈 Visual Analytics")
        if not fdf.empty:
            st.plotly_chart(create_yoy_chart(fdf), use_container_width=True)

        cc1, cc2 = st.columns(2)
        with cc1:
            fig = create_rep_chart(fdf)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        with cc2:
            fig = create_tier_chart(fdf)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

        fig = create_scatter_chart(fdf)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Accounts above the diagonal are growing; below are declining.")

    # TAB 3: Export
    with tab3:
        st.subheader("📤 Export Report")
        export_cols = ["Account", primary_col, comp_col,
                       "$ Change", "% Change", "Type", "Tier", "Sales Rep"]
        export_cols = [c for c in export_cols if c in fdf.columns]
        export_df   = fdf[export_cols].copy()

        ec1, ec2 = st.columns(2)
        with ec1:
            st.markdown("### Excel")
            st.write(f"{len(export_df)} accounts")
            st.download_button(
                "Download Excel",
                data=export_to_excel(export_df),
                file_name=f"sales_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with ec2:
            st.markdown("### CSV")
            st.write(f"{len(export_df)} accounts")
            st.download_button(
                "Download CSV",
                data=export_df.to_csv(index=False),
                file_name=f"sales_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

    # TAB 4: Data Audit
    with tab4:
        audit = st.session_state.get("audit")
        if not audit:
            st.info("Run a report to see the data audit.")
        else:
            st.subheader("🔍 Data Audit")

            total   = audit["total_raw"]
            kept    = audit["included"]
            dropped = total - kept

            a1, a2, a3, a4 = st.columns(4)
            with a1: st.metric("Raw Orders",       f"{total:,}")
            with a2: st.metric("Included",         f"{kept:,}",
                                delta=f"{kept/total*100:.1f}%" if total else "0%")
            with a3: st.metric("Excluded",         f"{dropped:,}",
                                delta=f"-{dropped/total*100:.1f}%" if total else "0%",
                                delta_color="inverse")
            with a4: st.metric("Unique Companies", f"{audit.get('unique_companies',0):,}")

            if "Tier" in df.columns:
                untiered = df["Tier"].eq("").sum() + df["Tier"].isna().sum()
                if untiered:
                    st.warning(
                        f"{untiered} accounts have no HubSpot tier. "
                        "Use Tier -> (No Tier) filter to view them."
                    )

            st.divider()
            st.markdown("#### Exclusion Breakdown")
            excl_df = pd.DataFrame({
                "Reason": [
                    "Shopify / Retail (B2C)",
                    "Excluded email domain (employees)",
                    "Outside selected date windows",
                    "$0 total orders",
                    "No company name",
                ],
                "Count": [
                    audit["excluded_source"],
                    audit.get("excluded_domain", 0),
                    audit["excluded_no_period"],
                    audit["excluded_zero_total"],
                    audit["unknown_company"],
                ],
            })
            excl_df["% of Raw"] = excl_df["Count"].apply(
                lambda x: f"{x/total*100:.1f}%" if total else "0%")
            st.dataframe(excl_df, use_container_width=True, hide_index=True)

            if audit.get("excluded_domain_counts"):
                st.markdown("#### Excluded by Email Domain")
                dom_df = pd.DataFrame([
                    {"Domain": k, "Orders Dropped": v}
                    for k, v in sorted(
                        audit["excluded_domain_counts"].items(), key=lambda x: -x[1])
                ])
                st.dataframe(dom_df, use_container_width=True, hide_index=True)

            if audit["excluded_sources"]:
                st.markdown("#### Excluded Source Values")
                src_df = pd.DataFrame([
                    {"Source": k, "Orders Dropped": v}
                    for k, v in sorted(
                        audit["excluded_sources"].items(), key=lambda x: -x[1])
                ])
                st.dataframe(src_df, use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("#### Orders and Revenue by Period")
            period_rows = [
                {
                    "Period":          lbl,
                    "Included Orders": stats["included"],
                    "Revenue":         f"${stats['revenue']:,.2f}",
                    "Excluded (B2C)":  stats["excluded_source"],
                }
                for lbl, stats in audit["by_period"].items()
            ]
            st.dataframe(pd.DataFrame(period_rows), use_container_width=True, hide_index=True)

            if audit["sample_excluded"]:
                st.divider()
                st.markdown("#### Sample Excluded Orders (first 10)")
                st.dataframe(pd.DataFrame(audit["sample_excluded"]),
                             use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown(
        f"<p style='text-align:center;color:#666;font-size:0.8rem;'>"
        f"Powered by {BRANDING['company_name']} | Management Reports</p>",
        unsafe_allow_html=True,
    )


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    main()
