"""
OrderFloz — Management Reporting Dashboard
==========================================
Standalone sales intelligence reporting:
- Year-over-Year sales comparison by account
- Filter by Sales Rep, Tier
- Export to Excel
- Charts & visualizations
"""

import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
from pathlib import Path
import io

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="OrderFloz Reports",
    page_icon="📊",
    layout="wide"
)

# =============================================================================
# CONSTANTS & CONFIG
# =============================================================================
CONFIG_FILE      = Path(".orderfloz_reports_config.json")
CACHE_META_FILE  = Path(".orderfloz_cache_meta.json")
CACHE_DIR        = Path(".orderfloz_cache")
CACHE_DIR.mkdir(exist_ok=True)

# How long HubSpot data is considered fresh before re-fetching
HUBSPOT_TTL_HOURS = 4

BRANDING = {
    "company_name": "OrderFloz",
    "primary_color": "#1a5276",
    "accent_color": "#00d4aa"
}

CURRENT_YEAR  = datetime.now().year
ANALYSIS_YEARS = [CURRENT_YEAR - 2, CURRENT_YEAR - 1, CURRENT_YEAR]

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
    Load email domains whose orders should be excluded (employees, internal accounts).
    Priority: config file (saved via sidebar) → Streamlit secrets → default.
    Example: "vivantskincare.com, monemtech.com"
    Always lowercase.
    """
    raw = load_config().get("excluded_domains", "") or get_secret("EXCLUDED_DOMAINS", "")
    if not raw:
        return set()
    return {d.strip().lower() for d in raw.split(",") if d.strip()}

# =============================================================================
# DISK CACHE  (orders + HubSpot)
# =============================================================================
import pickle
import hashlib

def _cache_key(label: str) -> str:
    """Stable filename-safe key for a period label."""
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
    """Persist orders list + fingerprint. Never saves empty results."""
    if not orders:          # empty = broken fetch; do not poison cache
        return
    try:
        key  = _cache_key(label)
        path = CACHE_DIR / f"orders_{key}.pkl"
        with open(path, "wb") as f:
            pickle.dump(orders, f)
        meta = _load_cache_meta()
        meta[f"cin7_{key}"] = {"label": label, "fingerprint": fingerprint,
                                "saved_at": datetime.now().isoformat(),
                                "count": len(orders)}
        _save_cache_meta(meta)
    except Exception:
        pass

def cache_load_orders(label: str, fingerprint: str):
    """
    Return cached orders if fingerprint matches, else None.
    Rejects empty caches (poisoned from a prior failed fetch).
    For closed periods (end < today) fingerprint is ignored — data can never change.
    """
    try:
        key  = _cache_key(label)
        meta = _load_cache_meta()
        entry = meta.get(f"cin7_{key}")
        if not entry:
            return None
        # Reject anything cached with zero orders
        if entry.get("count", 1) == 0:
            return None
        path = CACHE_DIR / f"orders_{key}.pkl"
        if not path.exists():
            return None
        if entry["fingerprint"] == fingerprint or fingerprint == "CLOSED":
            data = pickle.load(open(path, "rb"))
            # Double-check loaded data isn't empty
            if not data:
                return None
            return data
    except Exception:
        pass
    return None

def cache_save_hubspot(tiers: dict, owners: dict):
    """Persist HubSpot company data with timestamp."""
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
        saved_at = datetime.fromisoformat(entry["saved_at"])
        age_hours = (datetime.now() - saved_at).total_seconds() / 3600
        if age_hours > HUBSPOT_TTL_HOURS:
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
    """Wipe all cached data (manual override)."""
    try:
        for f in CACHE_DIR.iterdir():
            f.unlink()
        if CACHE_META_FILE.exists():
            CACHE_META_FILE.unlink()
    except Exception:
        pass

def cache_purge_empty_entries():
    """On startup, remove any cached periods that stored 0 orders (poisoned cache)."""
    try:
        meta = _load_cache_meta()
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
# SESSION STATE
# =============================================================================
if 'report_data' not in st.session_state:
    st.session_state.report_data = None
if 'cin7_orders_cache' not in st.session_state:
    st.session_state.cin7_orders_cache = None
if 'hubspot_companies_cache' not in st.session_state:
    st.session_state.hubspot_companies_cache = None
if 'audit' not in st.session_state:
    st.session_state.audit = None
if 'config_loaded' not in st.session_state:
    st.session_state.config_loaded = load_config()

# Auto-remove any zero-order cache entries from prior broken runs
cache_purge_empty_entries()

# =============================================================================
# CIN7 API FUNCTIONS
# =============================================================================
def test_cin7_connection(username: str, api_key: str) -> tuple:
    """Test Cin7 API credentials."""
    try:
        r = requests.get(
            "https://api.cin7.com/api/v1/SalesOrders",
            auth=(username, api_key),
            params={"rows": 1},
            timeout=15
        )
        if r.status_code == 200:
            return True, "Connected"
        elif r.status_code == 401:
            return False, "Invalid credentials"
        else:
            return False, f"Error {r.status_code}"
    except Exception as e:
        return False, str(e)

def probe_cin7_fingerprint(username: str, api_key: str,
                           start_date: str, end_date: str) -> str:
    """
    Fetch ONE order (latest modified) for a date range to get a change fingerprint.
    Returns "id:modifiedDate" string.  Fast — single API call, 1 row.
    """
    try:
        r = requests.get(
            "https://api.cin7.com/api/v1/SalesOrders",
            auth=(username, api_key),
            params={
                "where": f"createdDate >= '{start_date}' AND createdDate <= '{end_date}'",
                "rows": 1,
                "order": "modifiedDate desc",
                "fields": "id,modifiedDate"
            },
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            if data:
                o = data[0]
                return f"{o.get('id','')}:{o.get('modifiedDate','')}"
    except Exception:
        pass
    return ""

# Only fetch the fields we actually use — cuts payload by ~85%
# NOTE: if Cin7 rejects the fields param, we fall back to full fetch automatically
CIN7_FIELDS = "id,company,billingCompany,firstName,lastName,email,total,createdDate,modifiedDate,salesPersonEmail,source"

def fetch_orders_by_date_range(username: str, api_key: str,
                               start_date: str, end_date: str,
                               label: str = "",
                               progress_callback=None) -> list:
    """Fetch all orders between two dates from Cin7."""
    all_orders = []
    page = 1
    use_fields = True  # try slim fetch first; fall back to full if rejected

    while True:
        if progress_callback:
            progress_callback(f"Fetching {label} orders... page {page} ({len(all_orders)} so far)")
        try:
            params = {
                "where": f"createdDate >= '{start_date}' AND createdDate <= '{end_date}'",
                "page":  page,
                "rows":  250,
            }
            if use_fields:
                params["fields"] = CIN7_FIELDS

            r = requests.get(
                "https://api.cin7.com/api/v1/SalesOrders",
                auth=(username, api_key),
                params=params,
                timeout=60
            )

            # If fields param caused a 400/422, retry without it
            if r.status_code in (400, 422) and use_fields:
                use_fields = False
                continue

            if r.status_code != 200:
                st.warning(f"Cin7 returned {r.status_code} on page {page}: {r.text[:200]}")
                break

            orders = r.json()
            if not orders:
                break
            all_orders.extend(orders)
            if len(orders) < 250:
                break
            page += 1
        except Exception as e:
            st.warning(f"Error fetching page {page}: {e}")
            break
    return all_orders


def fetch_all_periods_parallel(username: str, api_key: str, periods: list,
                                status_placeholder=None) -> list:
    """
    Fetch all periods in parallel using threads.
    Returns combined flat list of all orders.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = {}

    def fetch_one(p):
        start_str = p["start"].strftime("%Y-%m-%dT00:00:00Z")
        end_str   = p["end"].strftime("%Y-%m-%dT23:59:59Z")
        orders = fetch_orders_by_date_range(username, api_key, start_str, end_str, label=p["label"])
        return p["label"], orders

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_one, p): p for p in periods}
        for future in as_completed(futures):
            label, orders = future.result()
            results[label] = orders
            if status_placeholder:
                done = len(results)
                status_placeholder.text(f"Fetching orders... {done}/{len(periods)} periods complete")

    # Flatten in period order
    all_orders = []
    for p in periods:
        all_orders.extend(results.get(p["label"], []))
    return all_orders
def aggregate_orders_by_company(orders: list, periods: list) -> tuple:
    """
    Aggregate orders by company name across dynamic periods.
    Excludes:
      - Retail / Shopify channel orders (B2C)
      - Orders where the customer email matches an excluded domain (employees)
    Returns: (company_data dict, audit dict)
    """
    excluded_domains = get_excluded_domains()

    def get_period_label(date_str):
        if not date_str:
            return None
        try:
            order_date = datetime.fromisoformat(date_str[:10]).date()
        except:
            return None
        for p in periods:
            if p["start"] <= order_date <= p["end"]:
                return p["label"]
        return None

    def email_domain(email: str) -> str:
        email = (email or '').strip().lower()
        return email.split('@')[-1] if '@' in email else ''

    company_data  = {}
    period_labels = [p["label"] for p in periods]

    audit = {
        "total_raw":               len(orders),
        "included":                0,
        "excluded_source":         0,
        "excluded_domain":         0,
        "excluded_no_period":      0,
        "excluded_zero_total":     0,
        "unknown_company":         0,
        "by_period":               {lbl: {"included": 0, "excluded_source": 0,
                                           "revenue": 0.0} for lbl in period_labels},
        "excluded_sources":        {},
        "excluded_domain_counts":  {},   # domain → order count
        "sample_excluded":         [],
    }

    for order in orders:
        company = (order.get('company') or order.get('billingCompany') or '').strip()
        if not company:
            first   = order.get('firstName', '')
            last    = order.get('lastName', '')
            company = f"{first} {last}".strip() or 'Unknown'
            if company == 'Unknown':
                audit["unknown_company"] += 1

        total        = float(order.get('total') or 0)
        rep_email    = (order.get('salesPersonEmail') or '').strip()
        cust_email   = (order.get('email') or '').strip()
        created_date = order.get('createdDate', '')
        period_label = get_period_label(created_date)
        source       = (order.get('source') or '').strip()
        source_lower = source.lower()

        # ── Exclude: retail / Shopify ──────────────────────────────────
        if 'shopify' in source_lower or 'retail' in source_lower:
            audit["excluded_source"] += 1
            audit["excluded_sources"][source] = audit["excluded_sources"].get(source, 0) + 1
            if period_label and period_label in period_labels:
                audit["by_period"][period_label]["excluded_source"] += 1
            if len(audit["sample_excluded"]) < 10:
                audit["sample_excluded"].append({
                    "reason": f"source={source}", "company": company,
                    "total": total, "date": created_date[:10] if created_date else ""})
            continue

        # ── Exclude: employee email domains ───────────────────────────
        domain = email_domain(cust_email)
        if excluded_domains and domain in excluded_domains:
            audit["excluded_domain"] += 1
            audit["excluded_domain_counts"][domain] = \
                audit["excluded_domain_counts"].get(domain, 0) + 1
            if len(audit["sample_excluded"]) < 10:
                audit["sample_excluded"].append({
                    "reason": f"excluded domain ({domain})", "company": company,
                    "email": cust_email, "total": total,
                    "date": created_date[:10] if created_date else ""})
            continue

        # ── Exclude: outside date windows ─────────────────────────────
        if period_label is None or period_label not in period_labels:
            audit["excluded_no_period"] += 1
            continue

        if total == 0:
            audit["excluded_zero_total"] += 1

        if company not in company_data:
            company_data[company] = {'rep': rep_email, 'order_count': 0}
            for lbl in period_labels:
                company_data[company][lbl] = 0.0

        company_data[company][period_label] += total
        company_data[company]['order_count'] += 1
        if rep_email and not company_data[company]['rep']:
            company_data[company]['rep'] = rep_email

        audit["included"] += 1
        audit["by_period"][period_label]["included"] += 1
        audit["by_period"][period_label]["revenue"]  += total

    audit["unique_companies"] = len(company_data)
    return company_data, audit

# =============================================================================
# HUBSPOT API FUNCTIONS
# =============================================================================
def get_hubspot_headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

def test_hubspot_connection(api_key: str) -> tuple:
    """Test HubSpot API credentials."""
    try:
        r = requests.get(
            "https://api.hubapi.com/crm/v3/objects/companies",
            headers=get_hubspot_headers(api_key),
            params={"limit": 1},
            timeout=15
        )
        if r.status_code == 200:
            return True, "Connected"
        elif r.status_code == 401:
            return False, "Invalid API key"
        else:
            return False, f"Error {r.status_code}"
    except Exception as e:
        return False, str(e)

def fetch_hubspot_companies_with_tier(api_key: str, tier_property: str = "commission_tier",
                                       progress_callback=None) -> dict:
    """
    Fetch all companies from HubSpot with their Tier property.
    Returns dict: {company_name_upper: tier_value}
    """
    headers = get_hubspot_headers(api_key)
    companies = {}
    after = None
    page = 1
    
    while True:
        if progress_callback:
            progress_callback(f"Fetching HubSpot companies... page {page}")
        
        params = {
            "limit": 100,
            "properties": f"name,{tier_property}"
        }
        if after:
            params["after"] = after
        
        try:
            r = requests.get(
                "https://api.hubapi.com/crm/v3/objects/companies",
                headers=headers,
                params=params,
                timeout=30
            )
            if r.status_code != 200:
                break
            
            data = r.json()
            results = data.get('results', [])
            
            for company in results:
                props = company.get('properties', {})
                name = (props.get('name') or '').strip().upper()
                tier = props.get(tier_property, '') or ''
                if name:
                    companies[name] = tier
            
            # Check for more pages
            paging = data.get('paging', {})
            next_page = paging.get('next', {})
            after = next_page.get('after')
            
            if not after:
                break
            page += 1
            
        except Exception as e:
            st.warning(f"Error fetching HubSpot companies: {e}")
            break
    
    return companies

def fetch_hubspot_owners(api_key: str) -> dict:
    """
    Fetch all HubSpot owners (users).
    Returns dict: {owner_id: full_name}
    """
    headers = get_hubspot_headers(api_key)
    try:
        r = requests.get(
            "https://api.hubapi.com/crm/v3/owners",
            headers=headers,
            params={"limit": 100},
            timeout=15
        )
        if r.status_code != 200:
            return {}
        owners = {}
        for o in r.json().get('results', []):
            oid = str(o.get('id', ''))
            first = o.get('firstName', '')
            last = o.get('lastName', '')
            email = o.get('email', '')
            name = f"{first} {last}".strip() or email
            if oid:
                owners[oid] = name
        return owners
    except Exception as e:
        st.warning(f"Could not fetch HubSpot owners: {e}")
        return {}


def fetch_hubspot_company_data(api_key: str, tier_property: str = "commission_tier",
                               progress_callback=None) -> tuple:
    """
    Single HubSpot companies fetch that returns BOTH tiers and owner IDs.
    Replaces fetch_hubspot_companies_with_tier + fetch_hubspot_company_owners.
    Returns: (tiers_dict, company_owners_dict)
      tiers_dict:         {company_name_upper: tier_value}
      company_owners_dict:{company_name_upper: owner_id}
    """
    headers = get_hubspot_headers(api_key)
    tiers = {}
    owners = {}
    after = None
    page = 1

    while True:
        if progress_callback:
            progress_callback(f"Fetching HubSpot companies... page {page}")

        params = {
            "limit": 100,
            "properties": f"name,{tier_property},hubspot_owner_id"
        }
        if after:
            params["after"] = after

        try:
            r = requests.get(
                "https://api.hubapi.com/crm/v3/objects/companies",
                headers=headers,
                params=params,
                timeout=30
            )
            if r.status_code != 200:
                break

            data = r.json()
            for company in data.get('results', []):
                props = company.get('properties', {})
                name = (props.get('name') or '').strip().upper()
                if not name:
                    continue
                tiers[name]  = props.get(tier_property, '') or ''
                owner_id     = props.get('hubspot_owner_id') or ''
                if owner_id:
                    owners[name] = str(owner_id)

            paging = data.get('paging', {})
            after = paging.get('next', {}).get('after')
            if not after:
                break
            page += 1

        except Exception as e:
            st.warning(f"Error fetching HubSpot companies: {e}")
            break

    return tiers, owners


# =============================================================================
# DATA PROCESSING
# =============================================================================
def build_report_dataframe(company_data: dict, hubspot_tiers: dict, periods: list,
                           company_owners: dict = None, owners_lookup: dict = None) -> pd.DataFrame:
    """
    Build the management report DataFrame.
    Column names are derived directly from the selected period labels so
    'YTD Sales' / 'Prior Year Sales' always reflect what was actually selected.
    """
    rows = []

    labels           = [p["label"] for p in periods]
    primary_label    = labels[-1]
    comparison_label = labels[-2] if len(labels) >= 2 else None

    # Column names shown in the table — derived from real period labels
    primary_col    = primary_label
    comparison_col = comparison_label if comparison_label else "Comparison"

    for company, data in company_data.items():
        primary_sales    = data.get(primary_label, 0)
        comparison_sales = data.get(comparison_label, 0) if comparison_label else 0

        if primary_sales == 0 and comparison_sales == 0:
            continue

        tier = hubspot_tiers.get(company.upper(), '')

        cin7_rep = data.get('rep', '')
        if cin7_rep:
            rep = cin7_rep
        elif company_owners and owners_lookup:
            owner_id = company_owners.get(company.upper(), '')
            rep = owners_lookup.get(owner_id, '') if owner_id else ''
        else:
            rep = ''

        if comparison_sales > 0:
            change_pct = ((primary_sales - comparison_sales) / comparison_sales) * 100
        elif primary_sales > 0:
            change_pct = 100.0
        else:
            change_pct = 0.0
        change_dollars = primary_sales - comparison_sales

        rows.append({
            'Account':          company,
            primary_col:        primary_sales,
            comparison_col:     comparison_sales,
            '$ Change':         change_dollars,
            '% Change':         change_pct,
            'Tier':             tier,
            'Sales Rep':        rep,
            '_order_count':     data.get('order_count', 0),
            '_primary_col':     primary_col,
            '_comparison_col':  comparison_col,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(primary_col, ascending=False).reset_index(drop=True)
    return df

# =============================================================================
# EXPORT FUNCTIONS
# =============================================================================
def export_to_excel(df: pd.DataFrame) -> bytes:
    """Export DataFrame to Excel bytes."""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Sales Report', index=False)
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Sales Report']
        for idx, col in enumerate(df.columns):
            max_length = max(
                df[col].astype(str).map(len).max(),
                len(col)
            ) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
    
    return output.getvalue()

# =============================================================================
# CUSTOM CSS
# =============================================================================
def inject_css():
    st.markdown(f"""
    <style>
        .main-header {{
            text-align: center;
            padding: 1rem 0;
            margin-bottom: 2rem;
        }}
        .main-header h1 {{
            color: {BRANDING['primary_color']};
            margin: 0;
        }}
        .metric-row {{
            display: flex;
            gap: 1rem;
            margin-bottom: 1rem;
        }}
        .metric-card {{
            background: linear-gradient(135deg, #1a5276, #2980b9);
            border-radius: 0.75rem;
            padding: 1.25rem;
            color: white;
            flex: 1;
            text-align: center;
        }}
        .metric-value {{
            font-size: 2rem;
            font-weight: bold;
        }}
        .metric-label {{
            font-size: 0.875rem;
            opacity: 0.9;
        }}
        .positive {{ color: #00d4aa; }}
        .negative {{ color: #ff6b6b; }}
        .filter-section {{
            background: #f8f9fa;
            padding: 1rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
        }}
        div[data-testid="stDataFrame"] {{
            width: 100%;
        }}
    </style>
    """, unsafe_allow_html=True)

# =============================================================================
# CHART FUNCTIONS
# =============================================================================
def create_yoy_chart(df: pd.DataFrame, periods: list):
    """Top 15 accounts — primary vs comparison bar chart."""
    import plotly.graph_objects as go

    primary_col    = df['_primary_col'].iloc[0]    if '_primary_col'    in df.columns else 'Sales'
    comparison_col = df['_comparison_col'].iloc[0] if '_comparison_col' in df.columns else 'Comparison'

    top = df.nlargest(15, primary_col)
    fig = go.Figure()
    if comparison_col in df.columns:
        fig.add_trace(go.Bar(name=comparison_col, x=top['Account'],
                             y=top[comparison_col], marker_color='#3498db'))
    fig.add_trace(go.Bar(name=primary_col, x=top['Account'],
                         y=top[primary_col], marker_color='#00d4aa'))
    fig.update_layout(
        title=f'Top 15 Accounts — {primary_col} vs {comparison_col}',
        barmode='group', xaxis_tickangle=-45, height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

def create_rep_performance_chart(df: pd.DataFrame):
    """Sales rep performance chart — primary period."""
    import plotly.express as px
    primary_col = df['_primary_col'].iloc[0] if '_primary_col' in df.columns else 'Sales'
    rep_data = df.groupby('Sales Rep').agg(
        Sales=(primary_col, 'sum'), Accounts=('Account', 'count')
    ).reset_index()
    rep_data = rep_data[rep_data['Sales Rep'] != ''].sort_values('Sales', ascending=True)
    fig = px.bar(rep_data, y='Sales Rep', x='Sales', orientation='h',
                 title=f'{primary_col} by Rep', color='Sales',
                 color_continuous_scale='Blues',
                 labels={'Sales': f'{primary_col} ($)'})
    fig.update_layout(height=400, showlegend=False)
    return fig

def create_tier_breakdown_chart(df: pd.DataFrame):
    """Tier breakdown by primary period sales."""
    import plotly.express as px
    primary_col = df['_primary_col'].iloc[0] if '_primary_col' in df.columns else 'Sales'
    tier_data = df.groupby('Tier').agg(Sales=(primary_col, 'sum')).reset_index()
    tier_data = tier_data[tier_data['Tier'] != '']
    if tier_data.empty:
        return None
    fig = px.pie(tier_data, values='Sales', names='Tier',
                 title=f'{primary_col} by Tier',
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(height=400)
    return fig

def create_growth_scatter(df: pd.DataFrame, periods: list):
    """Primary vs comparison growth scatter."""
    import plotly.express as px
    primary_col    = df['_primary_col'].iloc[0]    if '_primary_col'    in df.columns else 'Sales'
    comparison_col = df['_comparison_col'].iloc[0] if '_comparison_col' in df.columns else 'Comparison'
    if comparison_col not in df.columns:
        return None
    plot_df = df[(df[primary_col] > 0) | (df[comparison_col] > 0)].copy()
    if plot_df.empty:
        return None
    fig = px.scatter(
        plot_df, x=comparison_col, y=primary_col,
        size=primary_col, color='Tier' if plot_df['Tier'].any() else None,
        hover_name='Account', hover_data={'$ Change': True, '% Change': True},
        title=f'{primary_col} vs {comparison_col}',
        labels={comparison_col: f'{comparison_col} ($)', primary_col: f'{primary_col} ($)'}
    )
    max_val = max(plot_df[comparison_col].max(), plot_df[primary_col].max())
    fig.add_shape(type='line', x0=0, y0=0, x1=max_val, y1=max_val,
                  line=dict(color='gray', dash='dash'))
    fig.update_layout(height=500)
    return fig

# =============================================================================
# MAIN APPLICATION
# =============================================================================
def main():
    inject_css()
    
    # Header
    st.markdown(f"""
    <div class="main-header">
        <h1>📊 {BRANDING['company_name']} Management Reports</h1>
        <p style="color: #666;">Sales Intelligence Dashboard</p>
    </div>
    """, unsafe_allow_html=True)
    
    # =========================================================================
    # SIDEBAR - Configuration
    # =========================================================================
    with st.sidebar:
        st.header("⚙️ Data Sources")

        # Cin7 Credentials — pre-filled from Streamlit Secrets if set
        st.subheader("📦 Cin7 API")
        cin7_user = st.text_input("Username", value=get_secret("CIN7_USERNAME"), key="cin7_user")
        cin7_key  = st.text_input("API Key",  value=get_secret("CIN7_API_KEY"), type="password", key="cin7_key")
        
        if cin7_user and cin7_key:
            if st.button("Test Cin7", key="test_cin7"):
                ok, msg = test_cin7_connection(cin7_user, cin7_key)
                if ok: st.success(f"✅ {msg}")
                else:  st.error(f"❌ {msg}")
        
        st.divider()
        
        # HubSpot Credentials — pre-filled from Streamlit Secrets if set
        st.subheader("🟠 HubSpot API")
        hubspot_key   = st.text_input("Private App Token", value=get_secret("HUBSPOT_API_KEY"), type="password", key="hubspot_key")
        tier_property = st.text_input("Tier Property Name", value=get_secret("HUBSPOT_TIER_PROPERTY", "commission_tier"), key="tier_prop",
                                       help="The internal name of your Tier property in HubSpot")
        
        if hubspot_key:
            if st.button("Test HubSpot", key="test_hs"):
                ok, msg = test_hubspot_connection(hubspot_key)
                if ok: st.success(f"✅ {msg}")
                else:  st.error(f"❌ {msg}")
        
        st.divider()

        # ── REPORT PERIOD ───────────────────────────────────────────────────
        st.subheader("📅 Report Period")

        today = datetime.now().date()
        cy = today.year
        cm = today.month

        def quarter_of(month):
            return (month - 1) // 3 + 1

        cq = quarter_of(cm)

        PERIOD_OPTIONS = [
            "This Month",
            "Last Month",
            "This Quarter",
            "Last Quarter",
            "Year to Date",
            "Last 12 Months",
            "This Year (Full)",
            "Last Year (Full)",
            "Last 30 Days",
            "Last 60 Days",
            "Last 90 Days",
            "Custom Range",
        ]

        _cfg = st.session_state.config_loaded
        _saved_period = _cfg.get("last_period", "Year to Date")
        _saved_p_idx  = PERIOD_OPTIONS.index(_saved_period) if _saved_period in PERIOD_OPTIONS else 4

        selected_period = st.selectbox("Primary Period", PERIOD_OPTIONS,
                                        index=_saved_p_idx, key="primary_period")

        custom_start, custom_end = None, None
        if selected_period == "Custom Range":
            custom_start = st.date_input("From", value=datetime(cy, 1, 1).date(), key="custom_start")
            custom_end   = st.date_input("To",   value=today, key="custom_end")

        def get_quarter_bounds(year, q):
            starts = {1: (1,1), 2: (4,1), 3: (7,1), 4: (10,1)}
            ends   = {1: (3,31), 2: (6,30), 3: (9,30), 4: (12,31)}
            s = starts[q]; e = ends[q]
            return datetime(year, s[0], s[1]).date(), datetime(year, e[0], e[1]).date()

        def resolve_primary(period_name):
            if period_name == "This Month":
                import calendar
                last_day = calendar.monthrange(cy, cm)[1]
                return (datetime(cy, cm, 1).strftime("%b %Y"),
                        datetime(cy, cm, 1).date(),
                        min(datetime(cy, cm, last_day).date(), today))
            elif period_name == "Last Month":
                lm = cm - 1 if cm > 1 else 12
                ly = cy if cm > 1 else cy - 1
                import calendar
                last_day = calendar.monthrange(ly, lm)[1]
                return (datetime(ly, lm, 1).strftime("%b %Y"),
                        datetime(ly, lm, 1).date(),
                        datetime(ly, lm, last_day).date())
            elif period_name == "This Quarter":
                s, e = get_quarter_bounds(cy, cq)
                return (f"Q{cq} {cy}", s, min(e, today))
            elif period_name == "Last Quarter":
                lq = cq - 1 if cq > 1 else 4
                ly = cy if cq > 1 else cy - 1
                s, e = get_quarter_bounds(ly, lq)
                return (f"Q{lq} {ly}", s, e)
            elif period_name == "Year to Date":
                return (f"{cy} YTD", datetime(cy, 1, 1).date(), today)
            elif period_name == "Last 12 Months":
                return ("Last 12 Months", today.replace(year=today.year - 1), today)
            elif period_name == "This Year (Full)":
                return (str(cy), datetime(cy, 1, 1).date(), datetime(cy, 12, 31).date())
            elif period_name == "Last Year (Full)":
                return (str(cy - 1), datetime(cy-1, 1, 1).date(), datetime(cy-1, 12, 31).date())
            elif period_name == "Last 30 Days":
                return ("Last 30 Days", today - timedelta(days=30), today)
            elif period_name == "Last 60 Days":
                return ("Last 60 Days", today - timedelta(days=60), today)
            elif period_name == "Last 90 Days":
                return ("Last 90 Days", today - timedelta(days=90), today)
            elif period_name == "Custom Range":
                label = f"{custom_start.strftime('%b %d')} – {custom_end.strftime('%b %d, %Y')}" \
                        if custom_start and custom_end else "Custom"
                return (label, custom_start or today, custom_end or today)

        # Resolve primary period first — comparison pickers need p_start/p_end as defaults
        p_label, p_start, p_end = resolve_primary(selected_period)

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

        # ── Compare Against ──────────────────────────────────────────
        COMPARE_OPTIONS = [
            "Same Period Last Year",
            "Previous Period",
            "Custom Comparison Range",
            "None",
        ]
        _saved_compare = st.session_state.config_loaded.get("last_compare", "Same Period Last Year")
        _saved_c_idx   = COMPARE_OPTIONS.index(_saved_compare) \
                         if _saved_compare in COMPARE_OPTIONS else 0

        compare_to = st.selectbox("Compare Against", COMPARE_OPTIONS,
                                   index=_saved_c_idx, key="compare_to")

        # Custom comparison date pickers — only shown when needed
        comp_custom_start, comp_custom_end = None, None
        if compare_to == "Custom Comparison Range":
            comp_custom_start = st.date_input(
                "Compare From",
                value=p_start.replace(year=p_start.year - 1),
                key="comp_custom_start"
            )
            comp_custom_end = st.date_input(
                "Compare To",
                value=p_end.replace(year=p_end.year - 1),
                key="comp_custom_end"
            )

        # Resolve comparison period
        if compare_to == "Same Period Last Year":
            cs, ce  = same_period_prior_year(p_start, p_end)
            c_label = p_label.replace(str(p_start.year), str(cs.year)) \
                      if str(p_start.year) in p_label else f"{p_label} (Prior Year)"
            comp = (c_label, cs, ce)

        elif compare_to == "Previous Period":
            delta = p_end - p_start
            ce    = p_start - timedelta(days=1)
            cs    = ce - delta
            c_label = f"{cs.strftime('%b %d')} – {ce.strftime('%b %d, %Y')}"
            comp = (c_label, cs, ce)

        elif compare_to == "Custom Comparison Range":
            if comp_custom_start and comp_custom_end:
                c_label = f"{comp_custom_start.strftime('%b %d')} – {comp_custom_end.strftime('%b %d, %Y')}"
                comp = (c_label, comp_custom_start, comp_custom_end)
            else:
                comp = None

        else:  # None
            comp = None

        # Build periods list
        if comp:
            periods = [
                {"label": comp[0], "start": comp[1], "end": comp[2]},
                {"label": p_label, "start": p_start, "end": p_end},
            ]
        else:
            periods = [
                {"label": p_label, "start": p_start, "end": p_end},
            ]

        # Show resolved dates preview
        st.caption(f"📅 **{p_label}:** {p_start.strftime('%b %d, %Y')} → {p_end.strftime('%b %d, %Y')}")
        if comp:
            p_days = (p_end - p_start).days + 1
            c_days = (comp[2] - comp[1]).days + 1
            st.caption(f"📅 **vs. {comp[0]}:** {comp[1].strftime('%b %d, %Y')} → {comp[2].strftime('%b %d, %Y')}")
            if p_days == c_days:
                st.caption(f"✅ {p_days}-day range, exact match")
            else:
                st.caption(f"ℹ️ {p_days}d vs {c_days}d")

        st.divider()
        st.subheader("🚫 Excluded Domains")
        st.caption("Orders from these email domains are excluded (employees, internal).")

        _excluded_cfg = load_config().get("excluded_domains", get_secret("EXCLUDED_DOMAINS", "vivantskincare.com"))
        excluded_input = st.text_area(
            "One domain per line",
            value="\n".join(d.strip() for d in _excluded_cfg.split(",") if d.strip()),
            height=80,
            key="excluded_domains_input",
            help="e.g. vivantskincare.com — any order with a matching customer email is excluded."
        )
        if st.button("💾 Save Exclusions", use_container_width=True):
            domains_csv = ", ".join(
                d.strip().lower() for d in excluded_input.splitlines() if d.strip())
            save_config({"excluded_domains": domains_csv})
            cache_clear_all()
            st.session_state.report_data = None
            st.success("Saved — run report to apply.")
            st.rerun()

        # Cache status
        meta = _load_cache_meta()
        cached_periods = [v["label"] for k, v in meta.items() if k.startswith("cin7_")]
        hs_entry = meta.get("hubspot")
        if cached_periods:
            age_info = ""
            if hs_entry:
                hs_age = (datetime.now() - datetime.fromisoformat(hs_entry["saved_at"])).total_seconds() / 3600
                age_info = f" · HubSpot {hs_age:.1f}h ago"
            st.caption(f"💾 Cache: {len(cached_periods)} period(s){age_info}")
            if st.button("🗑️ Clear Cache", use_container_width=True):
                cache_clear_all()
                st.session_state.report_data = None
                st.rerun()

        if st.button("🔄 Generate Report", type="primary",
                     use_container_width=True, disabled=not can_generate):
            with st.spinner("Checking for updates..."):
                progress_text = st.empty()

                save_config({"last_period": selected_period, "last_compare": compare_to})
                st.session_state.periods = periods

                today = datetime.now().date()
                all_orders   = []
                cache_hits   = 0
                cache_misses = 0

                # ── Per-period: probe → cache check → fetch if needed ──────
                for p in periods:
                    start_str = p["start"].strftime("%Y-%m-%dT00:00:00Z")
                    end_str   = p["end"].strftime("%Y-%m-%dT23:59:59Z")
                    label     = p["label"]

                    # Closed period = end date is in the past; data is immutable
                    is_closed = p["end"] < today

                    if is_closed:
                        # Try cache first — no probe needed for closed periods
                        cached = cache_load_orders(label, "CLOSED")
                        if cached is not None:
                            all_orders.extend(cached)
                            cache_hits += 1
                            st.sidebar.success(f"✅ {label}: {len(cached)} orders (cached)")
                            continue

                    # Open (or no cache) → probe for fingerprint
                    progress_text.text(f"Checking {label} for changes...")
                    fingerprint = probe_cin7_fingerprint(
                        cin7_user, cin7_key, start_str, end_str)

                    cached = cache_load_orders(label, fingerprint)
                    if cached is not None:
                        all_orders.extend(cached)
                        cache_hits += 1
                        st.sidebar.success(f"✅ {label}: {len(cached)} orders (cached)")
                    else:
                        # Full fetch needed
                        cache_misses += 1
                        progress_text.text(f"Downloading {label} orders...")
                        period_orders = fetch_orders_by_date_range(
                            cin7_user, cin7_key, start_str, end_str, label=label,
                            progress_callback=lambda msg: progress_text.text(msg)
                        )
                        cache_save_orders(label, period_orders,
                                          "CLOSED" if is_closed else fingerprint)
                        all_orders.extend(period_orders)
                        st.sidebar.info(f"🔄 {label}: {len(period_orders)} orders (fresh)")

                # ── HubSpot: TTL cache ──────────────────────────────────────
                hubspot_tiers, company_owners = cache_load_hubspot()
                owners_lookup = {}

                if hubspot_key:
                    if hubspot_tiers is not None:
                        st.sidebar.success(
                            f"✅ HubSpot: {len(hubspot_tiers)} companies (cached)")
                        owners_lookup = fetch_hubspot_owners(hubspot_key)
                    else:
                        progress_text.text("Fetching HubSpot data...")
                        hubspot_tiers, company_owners = fetch_hubspot_company_data(
                            hubspot_key, tier_property,
                            progress_callback=lambda msg: progress_text.text(msg)
                        )
                        owners_lookup = fetch_hubspot_owners(hubspot_key)
                        cache_save_hubspot(hubspot_tiers, company_owners)
                        st.sidebar.info(
                            f"🔄 HubSpot: {len(hubspot_tiers)} companies (fresh)")
                else:
                    hubspot_tiers  = {}
                    company_owners = {}

                # ── Build report ────────────────────────────────────────────
                progress_text.text("Building report...")
                company_data, audit = aggregate_orders_by_company(all_orders, periods)
                df = build_report_dataframe(
                    company_data, hubspot_tiers, periods, company_owners, owners_lookup)

                st.session_state.report_data             = df
                st.session_state.cin7_orders_cache       = all_orders
                st.session_state.hubspot_companies_cache = hubspot_tiers
                st.session_state.audit                   = audit

                summary = f"✅ Done — {cache_hits} period(s) from cache, {cache_misses} refreshed"
                progress_text.text(summary)
                st.rerun()
    
    # =========================================================================
    # MAIN CONTENT
    # =========================================================================
    df = st.session_state.report_data

    # Guard: clear stale cache if it's missing required structural columns
    REQUIRED_COLS = {'Account', '$ Change', '% Change', 'Tier', 'Sales Rep',
                     '_primary_col', '_comparison_col'}
    if df is not None and not REQUIRED_COLS.issubset(set(df.columns)):
        st.session_state.report_data = None
        df = None
    
    if df is None:
        st.info("👈 Configure your API credentials and click **Generate Report** to get started.")
        st.subheader("📋 What this report shows")
        st.markdown("""
        | Column | Description |
        |--------|-------------|
        | **Account** | Company / account name |
        | **YTD Sales** | Revenue for your selected primary period |
        | **Prior Year Sales** | Revenue for the comparison period |
        | **$ Change** | Dollar difference (YTD vs Prior) |
        | **% Change** | Percentage growth or decline |
        | **Tier** | Commission tier from HubSpot |
        | **Sales Rep** | Assigned sales representative |
        """)
        return
    
    # Derive actual column names from the dataframe
    primary_col    = df['_primary_col'].iloc[0]    if '_primary_col'    in df.columns else 'Sales'
    comparison_col = df['_comparison_col'].iloc[0] if '_comparison_col' in df.columns else 'Comparison'

    # -------------------------------------------------------------------------
    # FILTERS
    # -------------------------------------------------------------------------
    periods = st.session_state.get('periods', [])

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        all_reps = ['All'] + sorted([r for r in df['Sales Rep'].unique() if r])
        selected_rep = st.selectbox("Sales Rep", all_reps)
    with col2:
        assigned_tiers = sorted([t for t in df['Tier'].unique() if t])
        has_untiered   = df['Tier'].eq('').any() or df['Tier'].isna().any()
        tier_options   = ['All'] + assigned_tiers + (['(No Tier)'] if has_untiered else [])
        selected_tier  = st.selectbox("Tier", tier_options)
    with col3:
        min_ytd = st.number_input(f"Min {primary_col} ($)", min_value=0, value=0, step=500)
    with col4:
        sort_by = st.selectbox("Sort By", [
            f"{primary_col} ↓", f"{comparison_col} ↓",
            "% Change ↓", "% Change ↑", "Account A→Z"
        ])

    # Apply filters
    filtered_df = df.copy()
    if selected_rep  != 'All': filtered_df = filtered_df[filtered_df['Sales Rep'] == selected_rep]
    if selected_tier == '(No Tier)':
        filtered_df = filtered_df[filtered_df['Tier'].eq('') | filtered_df['Tier'].isna()]
    elif selected_tier != 'All':
        filtered_df = filtered_df[filtered_df['Tier'] == selected_tier]
    if min_ytd > 0: filtered_df = filtered_df[filtered_df[primary_col] >= min_ytd]

    sort_map = {
        f"{primary_col} ↓":    (primary_col,    False),
        f"{comparison_col} ↓": (comparison_col, False),
        "% Change ↓":          ("% Change",     False),
        "% Change ↑":          ("% Change",     True),
        "Account A→Z":         ("Account",      True),
    }
    scol, sasc = sort_map.get(sort_by, (primary_col, False))
    if scol in filtered_df.columns:
        filtered_df = filtered_df.sort_values(scol, ascending=sasc).reset_index(drop=True)

    # -------------------------------------------------------------------------
    # SUMMARY METRICS
    # -------------------------------------------------------------------------
    st.divider()

    total_primary    = filtered_df[primary_col].sum()
    total_comparison = filtered_df[comparison_col].sum() if comparison_col in filtered_df.columns else 0
    total_chg_d      = filtered_df['$ Change'].sum()
    total_chg_p      = ((total_primary - total_comparison) / total_comparison * 100) \
                        if total_comparison > 0 else 0
    growing          = (filtered_df['% Change'] > 0).sum()
    declining        = (filtered_df['% Change'] < 0).sum()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1: st.metric("Accounts",        f"{len(filtered_df):,}")
    with c2: st.metric(primary_col,       f"${total_primary:,.0f}")
    with c3: st.metric(comparison_col,    f"${total_comparison:,.0f}")
    with c4: st.metric("$ Change",        f"${total_chg_d:+,.0f}")
    with c5: st.metric("% Change",        f"{total_chg_p:+.1f}%")
    with c6: st.metric("↑ Growing  ↓ Declining", f"{growing}  /  {declining}")

    # -------------------------------------------------------------------------
    # TABS: Table | Charts | Export
    # -------------------------------------------------------------------------
    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Account Report", "📈 Charts", "📤 Export", "🔍 Data Audit"])

    # ------------------------------------------------------------------
    # TAB 1 — Account Report (exactly the 7 columns)
    # ------------------------------------------------------------------
    with tab1:
        # ── Date range header ─────────────────────────────────────────
        if len(periods) == 2:
            pri = periods[1]; cmp = periods[0]
            st.markdown(
                f"**{pri['label']}** &nbsp;·&nbsp; "
                f"{pri['start'].strftime('%b %d, %Y')} — {pri['end'].strftime('%b %d, %Y')}"
                f"&emsp;|&emsp;"
                f"**vs. {cmp['label']}** &nbsp;·&nbsp; "
                f"{cmp['start'].strftime('%b %d, %Y')} — {cmp['end'].strftime('%b %d, %Y')}",
                unsafe_allow_html=True
            )
        elif len(periods) == 1:
            pri = periods[0]
            st.markdown(
                f"**{pri['label']}** &nbsp;·&nbsp; "
                f"{pri['start'].strftime('%b %d, %Y')} — {pri['end'].strftime('%b %d, %Y')}"
            )
        st.caption(f"{len(filtered_df)} accounts")

        # ── Build display rows ────────────────────────────────────────
        display_cols = ['Account', primary_col, comparison_col, '$ Change', '% Change', 'Tier', 'Sales Rep']
        display_cols = [c for c in display_cols if c in filtered_df.columns]
        display_df   = filtered_df[display_cols].copy()

        display_df[primary_col]    = display_df[primary_col].apply(lambda x: f"${x:,.2f}")
        if comparison_col in display_df.columns:
            display_df[comparison_col] = display_df[comparison_col].apply(lambda x: f"${x:,.2f}")
        display_df['$ Change']     = display_df['$ Change'].apply(
            lambda x: f"+${x:,.2f}" if x >= 0 else f"-${abs(x):,.2f}")
        display_df['% Change']     = display_df['% Change'].apply(lambda x: f"{x:+.1f}%")

        st.dataframe(display_df, use_container_width=True, hide_index=True,
                     height=min(600, (len(display_df) + 1) * 35 + 38))

    # ------------------------------------------------------------------
    # TAB 2 — Charts
    # ------------------------------------------------------------------
    with tab2:
        st.subheader("📈 Visual Analytics")
        if len(filtered_df) > 0:
            st.plotly_chart(create_yoy_chart(filtered_df, periods), use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            if filtered_df['Sales Rep'].any():
                fig = create_rep_performance_chart(filtered_df)
                if fig: st.plotly_chart(fig, use_container_width=True)
        with col2:
            if filtered_df['Tier'].any():
                fig = create_tier_breakdown_chart(filtered_df)
                if fig: st.plotly_chart(fig, use_container_width=True)

        fig = create_growth_scatter(filtered_df, periods)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("📍 Accounts above the line are growing YTD vs prior year; below are declining")

    # ------------------------------------------------------------------
    # TAB 3 — Export
    # ------------------------------------------------------------------
    with tab3:
        st.subheader("📤 Export Report")
        export_cols = ['Account', primary_col, comparison_col, '$ Change', '% Change', 'Tier', 'Sales Rep']
        export_cols = [c for c in export_cols if c in filtered_df.columns]
        export_df   = filtered_df[export_cols].copy()

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### Excel")
            st.write(f"{len(export_df)} accounts")
            excel_data = export_to_excel(export_df)
            st.download_button(
                label="⬇️ Download Excel",
                data=excel_data,
                file_name=f"sales_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        with col2:
            st.markdown("### CSV")
            st.write(f"{len(export_df)} accounts")
            st.download_button(
                label="⬇️ Download CSV",
                data=export_df.to_csv(index=False),
                file_name=f"sales_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

    # ------------------------------------------------------------------
    # TAB 4 — Data Audit
    # ------------------------------------------------------------------
    with tab4:
        audit = st.session_state.get('audit')
        if not audit:
            st.info("Run a report first to see the data audit.")
        else:
            st.subheader("🔍 Data Audit — What Was Imported")

            # ── Top-level order counts ────────────────────────────────
            total   = audit['total_raw']
            kept    = audit['included']
            dropped = total - kept

            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric("Raw Orders Fetched",  f"{total:,}")
            with c2: st.metric("Included in Report",  f"{kept:,}",
                                delta=f"{kept/total*100:.1f}% of total" if total else "0%")
            with c3: st.metric("Dropped / Excluded",  f"{dropped:,}",
                                delta=f"-{dropped/total*100:.1f}%" if total else "0%",
                                delta_color="inverse")
            with c4: st.metric("Unique Companies",    f"{audit.get('unique_companies', 0):,}")

            # Untiered accounts callout
            if df is not None and 'Tier' in df.columns:
                untiered = df['Tier'].eq('').sum() + df['Tier'].isna().sum()
                if untiered:
                    st.warning(f"⚠️ **{untiered} accounts** have no HubSpot tier assigned. "
                               f"Use the **Tier → (No Tier)** filter to view them.")

            st.divider()

            # ── Breakdown by exclusion reason ─────────────────────────
            st.markdown("#### Why Orders Were Excluded")
            excl_data = {
                "Reason": [
                    "Shopify / Retail channel (B2C)",
                    "Excluded email domain (employees)",
                    "Outside selected date windows",
                    "$0 total orders",
                    "No company name (mapped to 'Unknown')",
                ],
                "Count": [
                    audit['excluded_source'],
                    audit.get('excluded_domain', 0),
                    audit['excluded_no_period'],
                    audit['excluded_zero_total'],
                    audit['unknown_company'],
                ]
            }
            excl_df = pd.DataFrame(excl_data)
            excl_df['% of Raw'] = excl_df['Count'].apply(
                lambda x: f"{x/total*100:.1f}%" if total else "0%")
            st.dataframe(excl_df, use_container_width=True, hide_index=True)

            if audit.get('excluded_domain_counts'):
                st.markdown("#### Excluded by Email Domain")
                dom_df = pd.DataFrame([
                    {"Domain": k, "Orders Dropped": v}
                    for k, v in sorted(audit['excluded_domain_counts'].items(),
                                       key=lambda x: -x[1])
                ])
                st.dataframe(dom_df, use_container_width=True, hide_index=True)
                st.caption("Manage domains in the sidebar under **🚫 Excluded Domains**.")

            # ── Source values that triggered exclusion ────────────────
            if audit['excluded_sources']:
                st.markdown("#### Excluded Source Values (from Cin7 `source` field)")
                src_df = pd.DataFrame([
                    {"Source Value": k, "Orders Dropped": v}
                    for k, v in sorted(audit['excluded_sources'].items(),
                                       key=lambda x: -x[1])
                ])
                st.dataframe(src_df, use_container_width=True, hide_index=True)
                st.caption("⚠️ If any of these sources should be included in B2B wholesale revenue, "
                           "let Sam know to adjust the source filter logic.")

            st.divider()

            # ── Per-period breakdown ──────────────────────────────────
            st.markdown("#### Orders & Revenue by Period")
            period_rows = []
            for lbl, stats in audit['by_period'].items():
                period_rows.append({
                    "Period":           lbl,
                    "Included Orders":  stats['included'],
                    "Revenue":          f"${stats['revenue']:,.2f}",
                    "Excluded (B2C)":   stats['excluded_source'],
                })
            st.dataframe(pd.DataFrame(period_rows), use_container_width=True, hide_index=True)

            st.divider()

            # ── Sample excluded orders ────────────────────────────────
            if audit['sample_excluded']:
                st.markdown("#### Sample Excluded Orders (first 10)")
                st.dataframe(pd.DataFrame(audit['sample_excluded']),
                             use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown(
        f"<p style='text-align: center; color: #666; font-size: 0.8rem;'>Powered by {BRANDING['company_name']} | 📊 Management Reports</p>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
