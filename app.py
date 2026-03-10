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
CONFIG_FILE = Path(".orderfloz_reports_config.json")

BRANDING = {
    "company_name": "OrderFloz",
    "primary_color": "#1a5276",
    "accent_color": "#00d4aa"
}

CURRENT_YEAR = datetime.now().year
ANALYSIS_YEARS = [CURRENT_YEAR - 2, CURRENT_YEAR - 1, CURRENT_YEAR]  # default fallback

# =============================================================================
# CONFIG PERSISTENCE
# =============================================================================
def load_config() -> dict:
    """Load saved config (date prefs) from disk."""
    try:
        if CONFIG_FILE.exists():
            return json.loads(CONFIG_FILE.read_text())
    except Exception:
        pass
    return {}

def save_config(data: dict):
    """Save config (date prefs) to disk."""
    try:
        existing = load_config()
        existing.update(data)
        CONFIG_FILE.write_text(json.dumps(existing))
    except Exception:
        pass

def get_secret(key: str, default: str = "") -> str:
    """Read from Streamlit secrets if available, else return default."""
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default

# =============================================================================
# SESSION STATE
# =============================================================================
if 'report_data' not in st.session_state:
    st.session_state.report_data = None
if 'cin7_orders_cache' not in st.session_state:
    st.session_state.cin7_orders_cache = None
if 'hubspot_companies_cache' not in st.session_state:
    st.session_state.hubspot_companies_cache = None
if 'config_loaded' not in st.session_state:
    st.session_state.config_loaded = load_config()

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

def fetch_orders_by_date_range(username: str, api_key: str,
                               start_date: str, end_date: str,
                               label: str = "",
                               progress_callback=None) -> list:
    """Fetch all orders between two dates from Cin7."""
    all_orders = []
    page = 1
    while True:
        if progress_callback:
            progress_callback(f"Fetching {label} orders... page {page} ({len(all_orders)} so far)")
        try:
            r = requests.get(
                "https://api.cin7.com/api/v1/SalesOrders",
                auth=(username, api_key),
                params={
                    "where": f"createdDate >= '{start_date}' AND createdDate <= '{end_date}'",
                    "page": page,
                    "rows": 250
                },
                timeout=60
            )
            if r.status_code != 200:
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


def fetch_all_orders_by_year(username: str, api_key: str, year: int,
                             progress_callback=None) -> list:
    """Fetch all orders for a specific year from Cin7."""
    start_date = f"{year}-01-01T00:00:00Z"
    end_date = f"{year}-12-31T23:59:59Z"
    
    all_orders = []
    page = 1
    
    while True:
        if progress_callback:
            progress_callback(f"Fetching {year} orders... page {page} ({len(all_orders)} so far)")
        
        try:
            r = requests.get(
                "https://api.cin7.com/api/v1/SalesOrders",
                auth=(username, api_key),
                params={
                    "where": f"createdDate >= '{start_date}' AND createdDate <= '{end_date}'",
                    "page": page,
                    "rows": 250
                },
                timeout=60
            )
            if r.status_code != 200:
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

def aggregate_orders_by_company(orders: list, periods: list) -> dict:
    """
    Aggregate orders by company name across dynamic periods.
    Returns dict: {company_name: {period_label: total, 'rep': sales_rep}}
    """
    # Build date->period label lookup
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

    company_data = {}
    period_labels = [p["label"] for p in periods]

    for order in orders:
        company = (order.get('company') or order.get('billingCompany') or '').strip()
        if not company:
            first = order.get('firstName', '')
            last = order.get('lastName', '')
            company = f"{first} {last}".strip() or 'Unknown'

        total = float(order.get('total') or 0)
        rep_email = (order.get('salesPersonEmail') or '').strip()
        created_date = order.get('createdDate', '')
        period_label = get_period_label(created_date)

        if company not in company_data:
            company_data[company] = {'rep': rep_email, 'order_count': 0}
            for lbl in period_labels:
                company_data[company][lbl] = 0.0

        source = (order.get('source') or '').lower()
        if 'shopify' not in source and 'retail' not in source:
            if period_label and period_label in period_labels:
                company_data[company][period_label] += total
                company_data[company]['order_count'] += 1
            if rep_email and not company_data[company]['rep']:
                company_data[company]['rep'] = rep_email

    return company_data

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


def fetch_hubspot_company_owners(api_key: str, progress_callback=None) -> dict:
    """
    Fetch all HubSpot companies with their assigned owner (hubspot_owner_id).
    Returns dict: {company_name_upper: owner_id}
    """
    headers = get_hubspot_headers(api_key)
    company_owners = {}
    after = None
    page = 1

    while True:
        if progress_callback:
            progress_callback(f"Fetching HubSpot company owners... page {page}")

        params = {"limit": 100, "properties": "name,hubspot_owner_id"}
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
                owner_id = props.get('hubspot_owner_id') or ''
                if name and owner_id:
                    company_owners[name] = str(owner_id)

            paging = data.get('paging', {})
            after = paging.get('next', {}).get('after')
            if not after:
                break
            page += 1

        except Exception as e:
            st.warning(f"Error fetching company owners: {e}")
            break

    return company_owners


# =============================================================================
# DATA PROCESSING
# =============================================================================
def build_report_dataframe(company_data: dict, hubspot_tiers: dict, periods: list,
                           company_owners: dict = None, owners_lookup: dict = None) -> pd.DataFrame:
    """
    Build the final report DataFrame combining Cin7 sales and HubSpot tiers.
    """
    rows = []
    labels = [p["label"] for p in periods]
    l1, l2, l3 = labels[0], labels[1], labels[2]

    for company, data in company_data.items():
        total_sales = sum(data.get(lbl, 0) for lbl in labels)
        if total_sales == 0:
            continue

        tier = hubspot_tiers.get(company.upper(), '')

        # Sales rep: prefer Cin7 salesPersonEmail, fall back to HubSpot company owner
        cin7_rep = data.get('rep', '')
        if cin7_rep:
            rep = cin7_rep
        elif company_owners and owners_lookup:
            owner_id = company_owners.get(company.upper(), '')
            rep = owners_lookup.get(owner_id, '') if owner_id else ''
        else:
            rep = ''
        s1 = data.get(l1, 0)
        s2 = data.get(l2, 0)
        s3 = data.get(l3, 0)

        change_s2_s1_pct = ((s2 - s1) / s1 * 100) if s1 > 0 else (100.0 if s2 > 0 else 0.0)
        change_s2_s1_dollars = s2 - s1
        change_s3_s2_pct = ((s3 - s2) / s2 * 100) if s2 > 0 else (100.0 if s3 > 0 else 0.0)
        change_s3_s2_dollars = s3 - s2

        rows.append({
            'Company': company,
            f'{l1} Sales': s1,
            f'{l2} Sales': s2,
            f'{l3} Sales': s3,
            f'{l2} vs {l1} ($)': change_s2_s1_dollars,
            f'{l2} vs {l1} (%)': change_s2_s1_pct,
            f'{l3} vs {l2} ($)': change_s3_s2_dollars,
            f'{l3} vs {l2} (%)': change_s3_s2_pct,
            'Total Sales': total_sales,
            'Sales Rep': rep,
            'Tier': tier,
            'Order Count': data.get('order_count', 0)
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values('Total Sales', ascending=False)
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
    """Create Year-over-Year comparison chart."""
    import plotly.graph_objects as go

    l1, l2, l3 = periods[0]["label"], periods[1]["label"], periods[2]["label"]
    top_companies = df.nlargest(15, 'Total Sales')

    fig = go.Figure()
    fig.add_trace(go.Bar(name=l1, x=top_companies['Company'], y=top_companies[f'{l1} Sales'], marker_color='#3498db'))
    fig.add_trace(go.Bar(name=l2, x=top_companies['Company'], y=top_companies[f'{l2} Sales'], marker_color='#2ecc71'))
    fig.add_trace(go.Bar(name=l3, x=top_companies['Company'], y=top_companies[f'{l3} Sales'], marker_color='#e74c3c'))

    fig.update_layout(
        title='Top 15 Accounts - Period over Period Sales',
        barmode='group',
        xaxis_tickangle=-45,
        height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

def create_rep_performance_chart(df: pd.DataFrame):
    """Create sales rep performance chart."""
    import plotly.express as px
    
    # Aggregate by rep
    rep_data = df.groupby('Sales Rep').agg({
        'Total Sales': 'sum',
        'Company': 'count'
    }).reset_index()
    rep_data.columns = ['Sales Rep', 'Total Sales', 'Account Count']
    rep_data = rep_data[rep_data['Sales Rep'] != '']  # Remove empty reps
    rep_data = rep_data.sort_values('Total Sales', ascending=True)
    
    fig = px.bar(
        rep_data,
        y='Sales Rep',
        x='Total Sales',
        orientation='h',
        title='Sales by Rep (All Time)',
        color='Total Sales',
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(height=400, showlegend=False)
    
    return fig

def create_tier_breakdown_chart(df: pd.DataFrame):
    """Create tier breakdown pie chart."""
    import plotly.express as px
    
    tier_data = df.groupby('Tier').agg({
        'Total Sales': 'sum'
    }).reset_index()
    tier_data = tier_data[tier_data['Tier'] != '']  # Remove empty tiers
    
    if tier_data.empty:
        return None
    
    fig = px.pie(
        tier_data,
        values='Total Sales',
        names='Tier',
        title='Sales by Tier',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    
    fig.update_layout(height=400)
    
    return fig

def create_growth_scatter(df: pd.DataFrame, periods: list):
    """Create growth scatter plot."""
    import plotly.express as px

    l2, l3 = periods[1]["label"], periods[2]["label"]
    growth_df = df[(df[f'{l2} Sales'] > 0) | (df[f'{l3} Sales'] > 0)].copy()

    if growth_df.empty:
        return None

    fig = px.scatter(
        growth_df,
        x=f'{l2} Sales',
        y=f'{l3} Sales',
        size='Total Sales',
        color='Tier' if growth_df['Tier'].any() else None,
        hover_name='Company',
        title=f'{l3} vs {l2}',
        labels={f'{l2} Sales': f'{l2} Sales ($)', f'{l3} Sales': f'{l3} Sales ($)'}
    )

    max_val = max(growth_df[f'{l2} Sales'].max(), growth_df[f'{l3} Sales'].max())
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
            ok, msg = test_cin7_connection(cin7_user, cin7_key)
            if ok:
                st.success(f"✅ {msg}")
            else:
                st.error(f"❌ {msg}")
        
        st.divider()
        
        # HubSpot Credentials — pre-filled from Streamlit Secrets if set
        st.subheader("🟠 HubSpot API")
        hubspot_key   = st.text_input("Private App Token", value=get_secret("HUBSPOT_API_KEY"), type="password", key="hubspot_key")
        tier_property = st.text_input("Tier Property Name", value=get_secret("HUBSPOT_TIER_PROPERTY", "commission_tier"), key="tier_prop",
                                       help="The internal name of your Tier property in HubSpot")
        
        if hubspot_key:
            ok, msg = test_hubspot_connection(hubspot_key)
            if ok:
                st.success(f"✅ {msg}")
            else:
                st.error(f"❌ {msg}")
        
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

        # Load last used preferences
        _cfg = st.session_state.config_loaded
        _saved_period  = _cfg.get("last_period", "Year to Date")
        _saved_compare = _cfg.get("last_compare", "Same Period Last Year")
        _saved_p_idx   = PERIOD_OPTIONS.index(_saved_period) if _saved_period in PERIOD_OPTIONS else 4
        _compare_opts  = ["Same Period Last Year", "Previous Period", "None"]
        _saved_c_idx   = _compare_opts.index(_saved_compare) if _saved_compare in _compare_opts else 0

        selected_period = st.selectbox("Primary Period", PERIOD_OPTIONS,
                                        index=_saved_p_idx, key="primary_period")

        compare_options = ["Same Period Last Year", "Previous Period", "None"]
        compare_to = st.selectbox("Compare Against", compare_options,
                                   index=_saved_c_idx, key="compare_to")

        # Custom range inputs — only shown when needed
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
            """Return (label, start, end) for the selected primary period."""
            if period_name == "This Month":
                import calendar
                last_day = calendar.monthrange(cy, cm)[1]
                return (
                    datetime(cy, cm, 1).strftime("%b %Y"),
                    datetime(cy, cm, 1).date(),
                    min(datetime(cy, cm, last_day).date(), today)
                )
            elif period_name == "Last Month":
                lm = cm - 1 if cm > 1 else 12
                ly = cy if cm > 1 else cy - 1
                import calendar
                last_day = calendar.monthrange(ly, lm)[1]
                return (
                    datetime(ly, lm, 1).strftime("%b %Y"),
                    datetime(ly, lm, 1).date(),
                    datetime(ly, lm, last_day).date()
                )
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
                return ("Last 12 Months",
                        (today.replace(year=today.year - 1)),
                        today)
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

        def resolve_comparison(primary_label, primary_start, primary_end, compare_name):
            """Return (label, start, end) for the comparison period."""
            if compare_name == "None":
                return None
            delta = primary_end - primary_start
            if compare_name == "Same Period Last Year":
                try:
                    cs = primary_start.replace(year=primary_start.year - 1)
                    ce = primary_end.replace(year=primary_end.year - 1)
                except ValueError:
                    cs = primary_start - timedelta(days=365)
                    ce = primary_end - timedelta(days=365)
                # Build a clean label
                if "YTD" in primary_label:
                    lbl = f"{primary_start.year - 1} YTD"
                elif primary_label.startswith("Q"):
                    parts = primary_label.split()
                    lbl = f"{parts[0]} {int(parts[1]) - 1}"
                else:
                    try:
                        lbl = str(int(primary_label.split()[0]) - 1)
                    except:
                        lbl = f"Prior Year"
                return (lbl, cs, ce)
            elif compare_name == "Previous Period":
                ce = primary_start - timedelta(days=1)
                cs = ce - delta
                lbl = f"Prev {primary_label}"
                return (lbl, cs, ce)

        # Resolve the periods
        p_label, p_start, p_end = resolve_primary(selected_period)
        comp = resolve_comparison(p_label, p_start, p_end, compare_to)

        # Build periods list: always show prior full year + comparison + primary
        prior_year = cy - 1
        prior_year_end = datetime(prior_year, 12, 31).date()

        if comp:
            periods = [
                {"label": comp[0], "start": comp[1], "end": comp[2]},
                {"label": p_label,  "start": p_start,  "end": p_end},
            ]
        else:
            periods = [
                {"label": p_label, "start": p_start, "end": p_end},
            ]

        # Always add a 3rd full prior year column for context if we only have 2
        if len(periods) == 2 and str(prior_year) not in [p["label"] for p in periods]:
            periods.insert(0, {
                "label": str(prior_year),
                "start": datetime(prior_year, 1, 1).date(),
                "end": prior_year_end
            })

        # Show resolved dates as a preview
        st.caption("**Resolved periods:**")
        for p in periods:
            st.caption(f"• **{p['label']}**: {p['start'].strftime('%b %d, %Y')} → {p['end'].strftime('%b %d, %Y')}")

        st.divider()

        # Generate Report Button
        can_generate = cin7_user and cin7_key

        if st.button("🔄 Generate Report", type="primary",
                     use_container_width=True, disabled=not can_generate):
            with st.spinner("Building report..."):
                progress_text = st.empty()

                # Save period preferences for next session
                save_config({
                    "last_period":  selected_period,
                    "last_compare": compare_to,
                })

                # Store period config in session state
                st.session_state.periods = periods

                # Fetch Cin7 orders for each period
                all_orders = []
                for p in periods:
                    start_str = p["start"].strftime("%Y-%m-%dT00:00:00Z")
                    end_str   = p["end"].strftime("%Y-%m-%dT23:59:59Z")
                    progress_text.text(f"Fetching {p['label']} orders...")
                    period_orders = fetch_orders_by_date_range(
                        cin7_user, cin7_key,
                        start_str, end_str,
                        label=p["label"],
                        progress_callback=lambda msg: progress_text.text(msg)
                    )
                    all_orders.extend(period_orders)
                    st.sidebar.info(f"📅 {p['label']}: {len(period_orders)} orders")
                
                # Aggregate by company
                progress_text.text("Aggregating by company...")
                company_data = aggregate_orders_by_company(all_orders, periods)
                
                # Fetch HubSpot tiers + owners
                hubspot_tiers = {}
                company_owners = {}
                owners_lookup = {}
                if hubspot_key:
                    progress_text.text("Fetching HubSpot tiers...")
                    hubspot_tiers = fetch_hubspot_companies_with_tier(
                        hubspot_key, tier_property,
                        progress_callback=lambda msg: progress_text.text(msg)
                    )
                    st.sidebar.info(f"🏢 HubSpot: {len(hubspot_tiers)} companies")

                    progress_text.text("Fetching HubSpot owners...")
                    owners_lookup = fetch_hubspot_owners(hubspot_key)
                    company_owners = fetch_hubspot_company_owners(
                        hubspot_key,
                        progress_callback=lambda msg: progress_text.text(msg)
                    )
                    st.sidebar.info(f"👤 Owners: {len(owners_lookup)} reps mapped")
                
                # Build report
                progress_text.text("Building report...")
                df = build_report_dataframe(company_data, hubspot_tiers, periods, company_owners, owners_lookup)
                
                st.session_state.report_data = df
                st.session_state.cin7_orders_cache = all_orders
                st.session_state.hubspot_companies_cache = hubspot_tiers
                
                progress_text.text("✅ Report ready!")
                st.rerun()
    
    # =========================================================================
    # MAIN CONTENT
    # =========================================================================
    df = st.session_state.report_data
    
    if df is None:
        st.info("👈 Configure your API credentials and click **Generate Report** to get started.")
        
        # Show sample of what they'll get
        st.subheader("📋 Report Preview")
        st.markdown("""
        This report will show you:
        
        | Column | Description |
        |--------|-------------|
        | **Company** | Account/Company name |
        | **2024 Sales** | Total sales in 2024 |
        | **2025 Sales** | Total sales in 2025 |
        | **2026 Sales (YTD)** | Year-to-date sales |
        | **$ Change** | Dollar change between periods |
        | **% Change** | Percentage change |
        | **Sales Rep** | Assigned sales representative |
        | **Tier** | Customer tier from HubSpot |
        """)
        return
    
    # -------------------------------------------------------------------------
    # FILTERS
    # -------------------------------------------------------------------------
    st.subheader("🔍 Filters")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Sales Rep filter
        all_reps = ['All'] + sorted([r for r in df['Sales Rep'].unique() if r])
        selected_rep = st.selectbox("Sales Rep", all_reps)
    
    with col2:
        # Tier filter
        all_tiers = ['All'] + sorted([t for t in df['Tier'].unique() if t])
        selected_tier = st.selectbox("Tier", all_tiers)
    
    with col3:
        # Minimum sales filter
        min_sales = st.number_input("Minimum Total Sales ($)", min_value=0, value=0, step=1000)
    
    # Apply filters
    filtered_df = df.copy()
    if selected_rep != 'All':
        filtered_df = filtered_df[filtered_df['Sales Rep'] == selected_rep]
    if selected_tier != 'All':
        filtered_df = filtered_df[filtered_df['Tier'] == selected_tier]
    if min_sales > 0:
        filtered_df = filtered_df[filtered_df['Total Sales'] >= min_sales]
    
    # -------------------------------------------------------------------------
    # SUMMARY METRICS
    # -------------------------------------------------------------------------
    st.divider()

    periods = st.session_state.get('periods', [
        {"label": str(CURRENT_YEAR - 2)}, {"label": str(CURRENT_YEAR - 1)}, {"label": f"{CURRENT_YEAR} YTD"}
    ])
    l1, l2, l3 = periods[0]["label"], periods[1]["label"], periods[2]["label"]

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("📊 Accounts", f"{len(filtered_df):,}")

    with col2:
        total_s1 = filtered_df[f'{l1} Sales'].sum()
        st.metric(f"💰 {l1}", f"${total_s1:,.0f}")

    with col3:
        total_s2 = filtered_df[f'{l2} Sales'].sum()
        change = ((total_s2 - total_s1) / total_s1 * 100) if total_s1 > 0 else 0
        st.metric(f"💰 {l2}", f"${total_s2:,.0f}", f"{change:+.1f}%")

    with col4:
        total_s3 = filtered_df[f'{l3} Sales'].sum()
        change = ((total_s3 - total_s2) / total_s2 * 100) if total_s2 > 0 else 0
        st.metric(f"💰 {l3}", f"${total_s3:,.0f}", f"{change:+.1f}% vs {l2}")

    with col5:
        total_all = filtered_df['Total Sales'].sum()
        st.metric("💎 Total Sales", f"${total_all:,.0f}")
    
    # -------------------------------------------------------------------------
    # TABS: Table | Charts
    # -------------------------------------------------------------------------
    st.divider()
    
    tab1, tab2, tab3 = st.tabs(["📋 Data Table", "📈 Charts", "📤 Export"])
    
    # TAB 1: Data Table
    with tab1:
        st.subheader(f"Sales Report ({len(filtered_df)} accounts)")
        
        # Format the dataframe for display
        display_df = filtered_df.copy()

        # Format currency columns dynamically
        currency_cols = [c for c in display_df.columns if 'Sales' in c or '($)' in c or c == 'Total Sales']
        for col in currency_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}" if isinstance(x, (int, float)) else x)

        pct_cols = [c for c in display_df.columns if '(%)' in c]
        for col in pct_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:+.1f}%" if isinstance(x, (int, float)) else x)
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=600
        )
    
    # TAB 2: Charts
    with tab2:
        st.subheader("📈 Visual Analytics")
        
        # YoY Comparison Chart
        if len(filtered_df) > 0:
            st.plotly_chart(create_yoy_chart(filtered_df, periods), use_container_width=True)
        
        # Two column layout for smaller charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Rep Performance
            if filtered_df['Sales Rep'].any():
                fig = create_rep_performance_chart(filtered_df)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Tier Breakdown
            if filtered_df['Tier'].any():
                fig = create_tier_breakdown_chart(filtered_df)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
        
        # Growth Scatter
        fig = create_growth_scatter(filtered_df, periods)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("📍 Companies above the dashed line are growing; below are declining")
    
    # TAB 3: Export
    with tab3:
        st.subheader("📤 Export Data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Excel Export")
            st.write(f"Export {len(filtered_df)} accounts to Excel")
            
            excel_data = export_to_excel(filtered_df)
            st.download_button(
                label="⬇️ Download Excel",
                data=excel_data,
                file_name=f"orderfloz_sales_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col2:
            st.markdown("### CSV Export")
            st.write(f"Export {len(filtered_df)} accounts to CSV")
            
            csv_data = filtered_df.to_csv(index=False)
            st.download_button(
                label="⬇️ Download CSV",
                data=csv_data,
                file_name=f"orderfloz_sales_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<p style='text-align: center; color: #666; font-size: 0.8rem;'>Powered by {BRANDING['company_name']} | 📊 Management Reports</p>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
