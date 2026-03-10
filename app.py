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

# Years to analyze
CURRENT_YEAR = datetime.now().year
ANALYSIS_YEARS = [CURRENT_YEAR - 2, CURRENT_YEAR - 1, CURRENT_YEAR]  # e.g., [2024, 2025, 2026]

# =============================================================================
# SESSION STATE
# =============================================================================
if 'report_data' not in st.session_state:
    st.session_state.report_data = None
if 'cin7_orders_cache' not in st.session_state:
    st.session_state.cin7_orders_cache = None
if 'hubspot_companies_cache' not in st.session_state:
    st.session_state.hubspot_companies_cache = None

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

def aggregate_orders_by_company(orders: list) -> dict:
    """
    Aggregate orders by company name.
    Returns dict: {company_name: {year: total, 'rep': sales_rep}}
    """
    company_data = {}
    
    for order in orders:
        company = (order.get('company') or order.get('billingCompany') or '').strip()
        if not company:
            # Use customer name if no company
            first = order.get('firstName', '')
            last = order.get('lastName', '')
            company = f"{first} {last}".strip() or 'Unknown'
        
        total = float(order.get('total') or 0)
        rep_email = (order.get('salesPersonEmail') or '').strip()
        
        # Get year from order date
        created_date = order.get('createdDate', '')
        if created_date:
            try:
                year = int(created_date[:4])
            except:
                year = CURRENT_YEAR
        else:
            year = CURRENT_YEAR
        
        # Initialize company if needed
        if company not in company_data:
            company_data[company] = {
                'rep': rep_email,
                'order_count': 0
            }
            for y in ANALYSIS_YEARS:
                company_data[company][y] = 0.0
        
        # Add to total (only count wholesale, skip retail)
        source = (order.get('source') or '').lower()
        if 'shopify' not in source and 'retail' not in source:
            if year in ANALYSIS_YEARS:
                company_data[company][year] += total
                company_data[company]['order_count'] += 1
            
            # Update rep if we have one and current is empty
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

def fetch_hubspot_companies_with_tier(api_key: str, tier_property: str = "tier",
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

# =============================================================================
# DATA PROCESSING
# =============================================================================
def build_report_dataframe(company_data: dict, hubspot_tiers: dict) -> pd.DataFrame:
    """
    Build the final report DataFrame combining Cin7 sales and HubSpot tiers.
    """
    rows = []
    
    for company, data in company_data.items():
        # Skip companies with no sales
        total_sales = sum(data.get(y, 0) for y in ANALYSIS_YEARS)
        if total_sales == 0:
            continue
        
        # Get tier from HubSpot (match by uppercase name)
        tier = hubspot_tiers.get(company.upper(), '')
        
        # Get year values
        y1, y2, y3 = ANALYSIS_YEARS
        sales_y1 = data.get(y1, 0)
        sales_y2 = data.get(y2, 0)
        sales_y3 = data.get(y3, 0)
        
        # Calculate changes
        # Y2 vs Y1
        if sales_y1 > 0:
            change_y2_y1_pct = ((sales_y2 - sales_y1) / sales_y1) * 100
        else:
            change_y2_y1_pct = 100.0 if sales_y2 > 0 else 0.0
        change_y2_y1_dollars = sales_y2 - sales_y1
        
        # Y3 vs Y2
        if sales_y2 > 0:
            change_y3_y2_pct = ((sales_y3 - sales_y2) / sales_y2) * 100
        else:
            change_y3_y2_pct = 100.0 if sales_y3 > 0 else 0.0
        change_y3_y2_dollars = sales_y3 - sales_y2
        
        rows.append({
            'Company': company,
            f'{y1} Sales': sales_y1,
            f'{y2} Sales': sales_y2,
            f'{y3} Sales (YTD)': sales_y3,
            f'{y2} vs {y1} ($)': change_y2_y1_dollars,
            f'{y2} vs {y1} (%)': change_y2_y1_pct,
            f'{y3} vs {y2} ($)': change_y3_y2_dollars,
            f'{y3} vs {y2} (%)': change_y3_y2_pct,
            'Total Sales': total_sales,
            'Sales Rep': data.get('rep', ''),
            'Tier': tier,
            'Order Count': data.get('order_count', 0)
        })
    
    df = pd.DataFrame(rows)
    
    # Sort by total sales descending
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
def create_yoy_chart(df: pd.DataFrame):
    """Create Year-over-Year comparison chart."""
    import plotly.graph_objects as go
    
    y1, y2, y3 = ANALYSIS_YEARS
    
    # Top 15 companies by total sales
    top_companies = df.nlargest(15, 'Total Sales')
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name=str(y1),
        x=top_companies['Company'],
        y=top_companies[f'{y1} Sales'],
        marker_color='#3498db'
    ))
    
    fig.add_trace(go.Bar(
        name=str(y2),
        x=top_companies['Company'],
        y=top_companies[f'{y2} Sales'],
        marker_color='#2ecc71'
    ))
    
    fig.add_trace(go.Bar(
        name=f'{y3} (YTD)',
        x=top_companies['Company'],
        y=top_companies[f'{y3} Sales (YTD)'],
        marker_color='#e74c3c'
    ))
    
    fig.update_layout(
        title='Top 15 Accounts - Year over Year Sales',
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

def create_growth_scatter(df: pd.DataFrame):
    """Create growth scatter plot."""
    import plotly.express as px
    
    y1, y2, y3 = ANALYSIS_YEARS
    
    # Filter to companies with sales in both recent years
    growth_df = df[(df[f'{y2} Sales'] > 0) | (df[f'{y3} Sales (YTD)'] > 0)].copy()
    
    if growth_df.empty:
        return None
    
    fig = px.scatter(
        growth_df,
        x=f'{y2} Sales',
        y=f'{y3} Sales (YTD)',
        size='Total Sales',
        color='Tier' if growth_df['Tier'].any() else None,
        hover_name='Company',
        title=f'{y3} YTD vs {y2} Full Year',
        labels={
            f'{y2} Sales': f'{y2} Sales ($)',
            f'{y3} Sales (YTD)': f'{y3} YTD Sales ($)'
        }
    )
    
    # Add reference line (45 degree = same sales)
    max_val = max(growth_df[f'{y2} Sales'].max(), growth_df[f'{y3} Sales (YTD)'].max())
    fig.add_shape(
        type='line',
        x0=0, y0=0,
        x1=max_val, y1=max_val,
        line=dict(color='gray', dash='dash')
    )
    
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
        
        # Cin7 Credentials
        st.subheader("📦 Cin7 API")
        cin7_user = st.text_input("Username", key="cin7_user")
        cin7_key = st.text_input("API Key", type="password", key="cin7_key")
        
        if cin7_user and cin7_key:
            ok, msg = test_cin7_connection(cin7_user, cin7_key)
            if ok:
                st.success(f"✅ {msg}")
            else:
                st.error(f"❌ {msg}")
        
        st.divider()
        
        # HubSpot Credentials
        st.subheader("🟠 HubSpot API")
        hubspot_key = st.text_input("Private App Token", type="password", key="hubspot_key")
        tier_property = st.text_input("Tier Property Name", value="tier", key="tier_prop",
                                       help="The internal name of your Tier property in HubSpot")
        
        if hubspot_key:
            ok, msg = test_hubspot_connection(hubspot_key)
            if ok:
                st.success(f"✅ {msg}")
            else:
                st.error(f"❌ {msg}")
        
        st.divider()
        
        # Generate Report Button
        can_generate = cin7_user and cin7_key
        
        if st.button("🔄 Generate Report", type="primary", 
                     use_container_width=True, disabled=not can_generate):
            with st.spinner("Building report..."):
                progress_text = st.empty()
                
                # Fetch Cin7 orders for each year
                all_orders = []
                for year in ANALYSIS_YEARS:
                    progress_text.text(f"Fetching {year} orders...")
                    year_orders = fetch_all_orders_by_year(
                        cin7_user, cin7_key, year,
                        progress_callback=lambda msg: progress_text.text(msg)
                    )
                    all_orders.extend(year_orders)
                    st.sidebar.info(f"📅 {year}: {len(year_orders)} orders")
                
                # Aggregate by company
                progress_text.text("Aggregating by company...")
                company_data = aggregate_orders_by_company(all_orders)
                
                # Fetch HubSpot tiers
                hubspot_tiers = {}
                if hubspot_key:
                    progress_text.text("Fetching HubSpot tiers...")
                    hubspot_tiers = fetch_hubspot_companies_with_tier(
                        hubspot_key, tier_property,
                        progress_callback=lambda msg: progress_text.text(msg)
                    )
                    st.sidebar.info(f"🏢 HubSpot: {len(hubspot_tiers)} companies")
                
                # Build report
                progress_text.text("Building report...")
                df = build_report_dataframe(company_data, hubspot_tiers)
                
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
    
    y1, y2, y3 = ANALYSIS_YEARS
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("📊 Accounts", f"{len(filtered_df):,}")
    
    with col2:
        total_y1 = filtered_df[f'{y1} Sales'].sum()
        st.metric(f"💰 {y1} Sales", f"${total_y1:,.0f}")
    
    with col3:
        total_y2 = filtered_df[f'{y2} Sales'].sum()
        change = ((total_y2 - total_y1) / total_y1 * 100) if total_y1 > 0 else 0
        st.metric(f"💰 {y2} Sales", f"${total_y2:,.0f}", f"{change:+.1f}%")
    
    with col4:
        total_y3 = filtered_df[f'{y3} Sales (YTD)'].sum()
        change = ((total_y3 - total_y2) / total_y2 * 100) if total_y2 > 0 else 0
        st.metric(f"💰 {y3} YTD", f"${total_y3:,.0f}", f"{change:+.1f}% vs {y2}")
    
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
        
        # Format currency columns
        currency_cols = [f'{y1} Sales', f'{y2} Sales', f'{y3} Sales (YTD)', 
                        f'{y2} vs {y1} ($)', f'{y3} vs {y2} ($)', 'Total Sales']
        for col in currency_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
        
        # Format percentage columns
        pct_cols = [f'{y2} vs {y1} (%)', f'{y3} vs {y2} (%)']
        for col in pct_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:+.1f}%")
        
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
            st.plotly_chart(create_yoy_chart(filtered_df), use_container_width=True)
        
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
        fig = create_growth_scatter(filtered_df)
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
