import dask
import dask.dataframe as dd #\n
# import matplotlib as plt# \n
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('Agg')
import seaborn as sns # \n
from dask.diagnostics import ProgressBar# \n
import pandas as pd #\n
import numpy as np
from datetime import datetime, timedelta #\n
import os 
from ToolCalls import ToolCallsExecutor
tool_executor = ToolCallsExecutor()


class run_clickable_questions:
    def __init__(self):
        pass

    def run_clickable(self, result_data, query, run, chart_name , csv_filename): # whole_data
        print("in refresh file!!!!!!!!!!")
        print("Hello C3PO - 1")
        print("here is the query",query)


#=======================================Oncology================================================================
        if query.lower() == 'weekly data health check report':
            print("ds&g weekly deck refresh called")

        if query.lower() == 'monthly data health check report':
            print("ds&g monthly deck refresh called")

        if query.lower() == 'what is the projection factor for trodelvy?':
            tro_projection_sql = f'''SELECT * 
        FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.nsp_projection_factor_iv('TRODELVY')
        ORDER BY 
            CASE CATEGORY
                WHEN 'ADC' THEN 1
                WHEN 'CHEMO' THEN 2
                WHEN 'FULVESTRANT' THEN 3
            END;'''
            
            tro_projection_python="""import pandas as pd
import plotly.graph_objects as go
import pickle
result = result_data.copy()
# MANDATORY: Save raw result as CSV FIRST
csv_filename = "Structured_Bot/output_files/trodelvy_projection_factor_data.csv"
result.to_csv(csv_filename, index=False)

# Create a bar chart
fig = go.Figure()
fig.add_trace(go.Bar(
    x=[result['DRUG'].iloc[0]],
    y=[result['PROJECTION_FACTOR'].iloc[0]],
    text=[f"{result['PROJECTION_FACTOR'].iloc[0]:.2f}"],
    textposition='outside',
    marker_color='#1f77b4'
))

fig.update_layout(
    title='TRODELVY Projection Factor',
    xaxis_title='Drug',
    yaxis_title='Projection Factor',
    font=dict(size=12),
    yaxis=dict(range=[0, max(3, result['PROJECTION_FACTOR'].iloc[0] * 1.2)])  # Ensure bar is visible with some headroom
)

# Save plot as pickle
filename = "Structured_Bot/output_files/trodelvy_projection_factor_chart.pkl"
with open(filename, 'wb') as f:
    pickle.dump(fig, f)
"""
            tro_projection_analysis = f''' Calculating the projection factor for TRODELVY. This involves analyzing IV drug projection factors using our NSP (National Sales Projection) data.'''

            # Run the code
            if run:
                exec(tro_projection_python)
            else:
                return tro_projection_sql,tro_projection_python,tro_projection_analysis
            
        if query.lower() == 'what is the quaterly trend of capture ratio for trodelvy?':
            tro_capture_sql = f'''SELECT * 
        FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.nsp_capture_ratio_iv(
            'TRODELVY',
            'QUARTERLY',
            NULL
        )
        ORDER BY period ASC;'''
            
            tro_capture_python="""import pandas as pd
import plotly.graph_objects as go
import pickle
result = result_data.copy()
# MANDATORY: Save raw result as CSV FIRST
csv_filename = "Structured_Bot/output_files/trodelvy_quarterly_capture_ratio_data.csv"
result.to_csv(csv_filename, index=False)

# Data validation and cleaning
result = result.copy()
result['capture_ratio_percent'] = pd.to_numeric(result['capture_ratio_percent'], errors='coerce')

# Sort the data by period
result = result.sort_values('period')

# Create the line chart
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=result['period_label'],
    y=result['capture_ratio_percent'],
    mode='lines+markers',
    name='Capture Ratio',
    line=dict(width=2),
    marker=dict(size=8)
))

fig.update_layout(
    title='Quarterly Capture Ratio Trend for TRODELVY',
    xaxis_title='Quarter',
    yaxis_title='Capture Ratio (%)',
    font=dict(size=12),
    hovermode='x unified'
)

# Add value labels on the points
fig.update_traces(
    texttemplate='%{y:.2f}%',
    textposition='top center'
)

# Update y-axis to show percentages
fig.update_layout(yaxis=dict(tickformat='.2f', ticksuffix='%'))

# Save plot as pickle
filename = "Structured_Bot/output_files/trodelvy_quarterly_capture_ratio_trend.pkl"
with open(filename, 'wb') as f:
    pickle.dump(fig, f)
"""
            tro_capture_analysis = f'''Analyzing the quarterly capture ratio trend for TRODELVY. I'll retrieve the data, calculate the capture ratios, and visualize the results.'''

            # Run the code
            if run:
                exec(tro_capture_python)
            else:
                return tro_capture_sql,tro_capture_python,tro_capture_analysis
            
        if query.lower() == 'show the monthly trend of capture ratio for enhertu?':
            enhertu_capture_sql = f'''SELECT * 
        FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.nsp_capture_ratio_iv(
            'ENHERTU',
            'MONTHLY',
            NULL
        )
        ORDER BY period ASC;'''
            
            enhertu_capture_python="""import pandas as pd
import plotly.graph_objects as go
import pickle

# MANDATORY: Save raw result as CSV FIRST
csv_filename = "Structured_Bot/output_files/enhertu_monthly_capture_ratio_data.csv"
result.to_csv(csv_filename, index=False)

# Data validation and cleaning
result = result.copy()
result['capture_ratio_percent'] = pd.to_numeric(result['capture_ratio_percent'], errors='coerce')

# Sort the data by period
result = result.sort_values('period')

# Create the line chart
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=result['period_label'],
    y=result['capture_ratio_percent'],
    mode='lines+markers',
    name='Capture Ratio',
    line=dict(width=2),
    marker=dict(size=8)
))

fig.update_layout(
    title='Monthly Trend of Capture Ratio for Enhertu',
    xaxis_title='Month',
    yaxis_title='Capture Ratio (%)',
    font=dict(size=12),
    hovermode='x unified'
)

# Add percentage labels on top of each point
fig.update_traces(
    texttemplate='%{y:.2f}%',
    textposition='top center'
)

# Rotate x-axis labels for better readability
fig.update_xaxes(tickangle=45)

# Set y-axis to start from 0
fig.update_yaxes(range=[0, max(result['capture_ratio_percent']) * 1.1])

# Save plot as pickle
filename = "Structured_Bot/output_files/enhertu_monthly_capture_ratio_trend.pkl"
with open(filename, 'wb') as f:
    pickle.dump(fig, f)
"""
            enhertu_capture_analysis = f'''I'll analyze the monthly trend of capture ratio for Enhertu using the NSP Capture Ratio Tool, then visualize the results and provide insights.'''

            # Run the code
            if run:
                exec(enhertu_capture_python)
            else:
                return enhertu_capture_sql,enhertu_capture_python,enhertu_capture_analysis
            
        if query.lower() == 'what is the current projection factor for abemaciclib?':
            abemaciclib_projection_sql = f'''SELECT * 
        FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.nsp_projection_factor_oral();'''
            
            abemaciclib_projection_python="""import pandas as pd
import plotly.graph_objects as go
import pickle
result = result_data.copy()
# MANDATORY: Save raw result as CSV FIRST
csv_filename = "Structured_Bot/output_files/abemaciclib_projection_factor_data.csv"
result.to_csv(csv_filename, index=False)

# Create a simple bar chart
fig = go.Figure()
fig.add_trace(go.Bar(
    x=['ABEMACICLIB'],
    y=[result['PROJECTION_FACTOR'].iloc[0]],
    text=[f"{result['PROJECTION_FACTOR'].iloc[0]:.2f}"],
    textposition='outside',
    marker_color='#1f77b4'
))

fig.update_layout(
    title='Current Projection Factor for ABEMACICLIB',
    xaxis_title='Drug',
    yaxis_title='Projection Factor',
    font=dict(size=12),
    yaxis=dict(range=[0, max(2, result['PROJECTION_FACTOR'].iloc[0] * 1.1)])  # Ensure bar is visible
)

# Save plot as pickle
filename = "Structured_Bot/output_files/abemaciclib_projection_factor_chart.pkl"
with open(filename, 'wb') as f:
    pickle.dump(fig, f)
"""
            abemaciclib_projection_analysis = f'''Calculating the projection factor for ABEMACICLIB. This involves analyzing the R12M (rolling 12 months) data to determine the adjustment factor for this oral drug.'''

            # Run the code
            if run:
                exec(abemaciclib_projection_python)
            else:
                return abemaciclib_projection_sql,abemaciclib_projection_python,abemaciclib_projection_analysis
            
        

        if query.lower() == 'what is the monthly trend of trodelvy mx claims ?':
            mx_claims_sql = f'''SELECT 
    DATE_TRUNC('month', mx.svc_dt) AS month, 
    COUNT(mx.mx_clam_id) AS total_claims 
FROM 
    `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` mx 
LEFT JOIN 
    `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_hcpcs_codes_mapped` hcpcs_map 
    ON mx.prcr_cd = hcpcs_map.procedure_code 
    AND hcpcs_map.bc_market = 'Y' 
LEFT JOIN 
    `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc_map 
    ON mx.ndc11 = ndc_map.product_code 
    AND ndc_map.bc_market = 'Y' 
WHERE 
    mx.pt_cycle_id = (
        SELECT MAX(pt_cycle_id) 
        FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`
    ) 
    AND (LOWER(hcpcs_map.regimen) LIKE '%trodelvy%' 
         OR LOWER(ndc_map.regimen) LIKE '%trodelvy%')
    AND mx.svc_dt IS NOT NULL
    AND mx.svc_dt <= LAST_DAY(
        ADD_MONTHS(
            DATE_TRUNC('MONTH', 
                (SELECT MAX(svc_dt) 
                 FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)
            ), 
            -2
        )
    )
GROUP BY 
    DATE_TRUNC('month', mx.svc_dt) 
ORDER BY 
    month'''
            
            mx_claims_python="""import pandas as pd
import plotly.graph_objects as go
import pickle

result = result_data.copy()

# Ensure the date is in the correct format
result['month'] = pd.to_datetime(result['month'])

# Sort the dataframe by date
result = result.sort_values('month')

result.to_csv('Structured_Bot/output_files/trodelvy_mx_claims_data.csv', index=False)

# Create the Plotly figure
fig = go.Figure()

# Add line plot with markers
fig.add_trace(go.Scatter(
    x=result['month'],
    y=result['total_claims'],
    mode='lines+markers+text',
    name='Total Claims',
    line=dict(color='blue', width=2),
    marker=dict(size=8),
    text=result['total_claims'],
    textposition='top center',
    textfont=dict(size=10)
))

# Update layout
fig.update_layout(
    title='Monthly Trend of Trodelvy Mx Claims',
    title_font_size=16,
    xaxis_title='Month',
    yaxis_title='Total Claims',
    xaxis_title_font_size=12,
    yaxis_title_font_size=12,
    width=1200,
    height=600,
    hovermode='x unified',
    showlegend=False,
    xaxis=dict(tickangle=45),
    plot_bgcolor='white',
    xaxis_showgrid=True,
    yaxis_showgrid=True,
    xaxis_gridcolor='rgba(128,128,128,0.3)',
    yaxis_gridcolor='rgba(128,128,128,0.3)'
)

# Save the Plotly figure as a pickle file
with open('Structured_Bot/output_files/trodelvy_mx_claims_trend.pkl', 'wb') as f:
    pickle.dump(fig, f)

# Summary statistics
summary = result['total_claims'].describe()

# Calculate month-over-month growth
result['mom_growth'] = result['total_claims'].pct_change() * 100

# Identify months with highest and lowest claims
max_claims_month = result.loc[result['total_claims'].idxmax()]
min_claims_month = result.loc[result['total_claims'].idxmin()]
"""
            mx_claims_analysis = f'''I'll analyze the monthly trend of Trodelvy Mx claims for you. Let's break this down into steps:
Query the data for Trodelvy Mx claims
Visualize the monthly trend
Provide insights based on the results
Let's start with the SQL query to retrieve the data:'''

            # Run the code
            if run:
                exec(mx_claims_python)
            else:
                return mx_claims_sql,mx_claims_python,mx_claims_analysis
        
        
        if query.lower() == 'what is the monthly trend of rx claims over the past 1 year?':
            rx_claims_sql = f'''WITH latest_date AS (
  SELECT MAX(fill_dt) AS max_date
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`
)
SELECT 
  DATE_TRUNC('month', rx.fill_dt) AS month,
  COUNT(rx.clam_id) AS total_claims
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` rx
CROSS JOIN latest_date
WHERE rx.clam_type = 'PD'
  AND rx.pt_cycle_id = (
    SELECT MAX(pt_cycle_id)
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`
  )
  AND rx.fill_dt BETWEEN DATE_TRUNC('MONTH', ADD_MONTHS(DATE_TRUNC('MONTH', latest_date.max_date), -13))
                      AND LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', latest_date.max_date), -2))
GROUP BY DATE_TRUNC('month', rx.fill_dt)
ORDER BY month'''
            
            rx_claims_python="""import pandas as pd
import plotly.graph_objects as go
import pickle

# Data validation and cleaning
result = result_data.copy()
result['month'] = pd.to_datetime(result['month'])
result['total_claims'] = pd.to_numeric(result['total_claims'], errors='coerce')

# Save data to CSV
result.to_csv('Structured_Bot/output_files/rx_claims_monthly_trend.csv', index=False)

# Create the Plotly figure
fig = go.Figure()

# Add line plot with markers
fig.add_trace(go.Scatter(
    x=result['month'],
    y=result['total_claims'],
    mode='lines+markers+text',
    name='Total Claims',
    line=dict(color='blue', width=2),
    marker=dict(size=8),
    text=[f'{v:,}' for v in result['total_claims']],
    textposition='top center',
    textfont=dict(size=10, family='Arial Black')
))

# Update layout
fig.update_layout(
    title={
        'text': 'Monthly Trend of Rx Claims (Past 12 Months)',
        'font': {'size': 16, 'family': 'Arial Black'}
    },
    xaxis_title='Month',
    yaxis_title='Total Claims',
    xaxis_title_font={'size': 12, 'family': 'Arial Black'},
    yaxis_title_font={'size': 12, 'family': 'Arial Black'},
    width=1200,
    height=600,
    hovermode='x unified',
    showlegend=False,
    xaxis=dict(tickangle=45),
    yaxis=dict(tickformat=','),
    plot_bgcolor='white',
    xaxis_showgrid=True,
    yaxis_showgrid=True,
    xaxis_gridcolor='rgba(128,128,128,0.3)',
    yaxis_gridcolor='rgba(128,128,128,0.3)'
)

# Save the Plotly figure as pickle
with open('Structured_Bot/output_files/rx_claims_monthly_trend.pkl', 'wb') as f:
    pickle.dump(fig, f)
"""
            rx_claims_analysis = f''' I'll analyze the monthly trend of Rx claims over the past year for you. Let's break this down into steps and use the appropriate tools to fetch the data, visualize it, and provide insights.
Query Breakdown: a. Fetch Rx claims data for the past year b. Aggregate claims by month c. Visualize the monthly trend d. Provide insights on the trend
Let's start by generating the SQL query to retrieve the data:'''

            # Run the code
            if run:
                exec(rx_claims_python)
            else:
                return rx_claims_sql,rx_claims_python,rx_claims_analysis  

        if query.lower() == 'what is the monthly trend of trodelvy mx claims ?':
            mx_claims_sql = f'''SELECT 
    DATE_TRUNC('month', mx.svc_dt) AS month, 
    COUNT(mx.mx_clam_id) AS total_claims 
FROM 
    `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` mx 
LEFT JOIN 
    `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_hcpcs_codes_mapped` hcpcs_map 
    ON mx.prcr_cd = hcpcs_map.procedure_code 
    AND hcpcs_map.bc_market = 'Y' 
LEFT JOIN 
    `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc_map 
    ON mx.ndc11 = ndc_map.product_code 
    AND ndc_map.bc_market = 'Y' 
WHERE 
    mx.pt_cycle_id = (
        SELECT MAX(pt_cycle_id) 
        FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`
    ) 
    AND (LOWER(hcpcs_map.regimen) LIKE '%trodelvy%' 
         OR LOWER(ndc_map.regimen) LIKE '%trodelvy%')
    AND mx.svc_dt IS NOT NULL
    AND mx.svc_dt <= LAST_DAY(
        ADD_MONTHS(
            DATE_TRUNC('MONTH', 
                (SELECT MAX(svc_dt) 
                 FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)
            ), 
            -2
        )
    )
GROUP BY 
    DATE_TRUNC('month', mx.svc_dt) 
ORDER BY 
    month'''
            
            mx_claims_python="""import pandas as pd
import plotly.graph_objects as go
import pickle

result = result_data.copy()

# Ensure the date is in the correct format
result['month'] = pd.to_datetime(result['month'])

# Sort the dataframe by date
result = result.sort_values('month')

result.to_csv('Structured_Bot/output_files/trodelvy_mx_claims_data.csv', index=False)

# Create the Plotly figure
fig = go.Figure()

# Add line plot with markers
fig.add_trace(go.Scatter(
    x=result['month'],
    y=result['total_claims'],
    mode='lines+markers+text',
    name='Total Claims',
    line=dict(color='blue', width=2),
    marker=dict(size=8),
    text=result['total_claims'],
    textposition='top center',
    textfont=dict(size=10)
))

# Update layout
fig.update_layout(
    title='Monthly Trend of Trodelvy Mx Claims',
    title_font_size=16,
    xaxis_title='Month',
    yaxis_title='Total Claims',
    xaxis_title_font_size=12,
    yaxis_title_font_size=12,
    width=1200,
    height=600,
    hovermode='x unified',
    showlegend=False,
    xaxis=dict(tickangle=45),
    plot_bgcolor='white',
    xaxis_showgrid=True,
    yaxis_showgrid=True,
    xaxis_gridcolor='rgba(128,128,128,0.3)',
    yaxis_gridcolor='rgba(128,128,128,0.3)'
)

# Save the Plotly figure as a pickle file
with open('Structured_Bot/output_files/trodelvy_mx_claims_trend.pkl', 'wb') as f:
    pickle.dump(fig, f)

# Summary statistics
summary = result['total_claims'].describe()

# Calculate month-over-month growth
result['mom_growth'] = result['total_claims'].pct_change() * 100

# Identify months with highest and lowest claims
max_claims_month = result.loc[result['total_claims'].idxmax()]
min_claims_month = result.loc[result['total_claims'].idxmin()]
"""
            mx_claims_analysis = f'''I'll analyze the monthly trend of Trodelvy Mx claims for you. Let's break this down into steps:
Query the data for Trodelvy Mx claims
Visualize the monthly trend
Provide insights based on the results
Let's start with the SQL query to retrieve the data:'''

            # Run the code
            if run:
                exec(mx_claims_python)
            else:
                return mx_claims_sql,mx_claims_python,mx_claims_analysis  

        if query.lower() == 'what are the monthly trends of total claims filed for breast cancer drugs over the past one year?':
            rx_mx_claims_sql = f'''WITH rx_claims AS (
  SELECT 
    DATE_TRUNC('month', rx.fill_dt) AS claim_month,
    COUNT(rx.clam_id) AS rx_claim_count
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` rx
  LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc_map
    ON rx.ndc11 = ndc_map.product_code
    AND ndc_map.bc_market = 'Y'
  WHERE rx.clam_type = 'PD'
    AND rx.pt_cycle_id = (SELECT MAX(pt_cycle_id) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)
    AND rx.fill_dt BETWEEN 
      DATE_TRUNC('MONTH', ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(fill_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)), -13))
    AND 
      LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(fill_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)), -2))
  GROUP BY DATE_TRUNC('month', rx.fill_dt)
),
mx_claims AS (
  SELECT 
    DATE_TRUNC('month', mx.svc_dt) AS claim_month,
    COUNT(mx.mx_clam_id) AS mx_claim_count
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` mx
  LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_hcpcs_codes_mapped` hcpcs_map
    ON mx.prcr_cd = hcpcs_map.procedure_code
    AND hcpcs_map.bc_market = 'Y'
  LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc_map
    ON mx.ndc11 = ndc_map.product_code
    AND ndc_map.bc_market = 'Y'
  WHERE mx.pt_cycle_id = (SELECT MAX(pt_cycle_id) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)
    AND mx.svc_dt BETWEEN 
      DATE_TRUNC('MONTH', ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(svc_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)), -13))
    AND 
      LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(svc_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)), -2))
  GROUP BY DATE_TRUNC('month', mx.svc_dt)
)
SELECT 
  COALESCE(rx_claims.claim_month, mx_claims.claim_month) AS claim_month,
  COALESCE(rx_claims.rx_claim_count, 0) + COALESCE(mx_claims.mx_claim_count, 0) AS total_claims
FROM rx_claims
FULL OUTER JOIN mx_claims ON rx_claims.claim_month = mx_claims.claim_month
ORDER BY claim_month'''
            
            rx_mx_claims_python="""import pandas as pd
import plotly.graph_objects as go
import pickle

result = result_data.copy()

# Ensure the claim_month is in datetime format
result['claim_month'] = pd.to_datetime(result['claim_month'])

# Sort the dataframe by claim_month
result = result.sort_values('claim_month')

result.to_csv('Structured_Bot/output_files/breast_cancer_claims_data.csv', index=False)

# Create the Plotly figure
fig = go.Figure()

# Add line plot with markers
fig.add_trace(go.Scatter(
    x=result['claim_month'],
    y=result['total_claims'],
    mode='lines+markers+text',
    name='Total Claims',
    line=dict(color='blue', width=2),
    marker=dict(size=8),
    text=[f'{v:,}' for v in result['total_claims']],
    textposition='top center',
    textfont=dict(size=10)
))

# Update layout
fig.update_layout(
    title='Monthly Trends of Total Claims Filed for Breast Cancer Drugs',
    title_font_size=16,
    xaxis_title='Month',
    yaxis_title='Total Claims',
    xaxis_title_font_size=12,
    yaxis_title_font_size=12,
    width=1200,
    height=600,
    hovermode='x unified',
    showlegend=False,
    xaxis=dict(
        tickangle=45,
        tickformat='%b %Y'
    ),
    plot_bgcolor='white',
    xaxis_showgrid=True,
    yaxis_showgrid=True,
    xaxis_gridcolor='rgba(128,128,128,0.3)',
    yaxis_gridcolor='rgba(128,128,128,0.3)'
)

# Save the Plotly figure as a pickle file
with open('Structured_Bot/output_files/monthly_breast_cancer_drug_claims.pkl', 'wb') as f:
    pickle.dump(fig, f)

# Calculate some statistics for insights
total_claims = result['total_claims'].sum()
avg_monthly_claims = result['total_claims'].mean()
max_claims = result['total_claims'].max()
min_claims = result['total_claims'].min()
max_month = result.loc[result['total_claims'] == max_claims, 'claim_month'].iloc[0]
min_month = result.loc[result['total_claims'] == min_claims, 'claim_month'].iloc[0]
"""
            rx_mx_claims_analysis = f''' I'll analyze the monthly trends of total claims filed for breast cancer drugs over the past year. Let's break this down into steps and generate the necessary SQL query and visualization.
Analysis Plan:
Query the data for both RX and MX claims for breast cancer drugs over the past year.
Aggregate the claims by month.
Visualize the monthly trend.
Provide insights based on the results.
Let's start with the SQL query to retrieve the data:'''

            # Run the code
            if run:
                exec(rx_mx_claims_python)
            else:
                return rx_mx_claims_sql,rx_mx_claims_python,rx_mx_claims_analysis  

        if query.lower() == 'what is the monthly trend of cdk claims over the past year?':
            cdk_claims_sql = f'''WITH rx_claims AS (
  SELECT 
    DATE_TRUNC('month', rx.fill_dt) AS month,
    COUNT(rx.clam_id) AS total_claims
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` rx
  LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc
    ON rx.ndc11 = ndc.product_code
  WHERE rx.clam_type = 'PD'
    AND rx.pt_cycle_id = (SELECT MAX(pt_cycle_id) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)
    AND ndc.regimen = 'CDK'
    AND rx.fill_dt BETWEEN 
      DATE_TRUNC('MONTH', ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(fill_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)), -13))
    AND 
      LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(fill_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)), -2))
  GROUP BY DATE_TRUNC('month', rx.fill_dt)
),
mx_claims AS (
  SELECT 
    DATE_TRUNC('month', mx.svc_dt) AS month,
    COUNT(mx.mx_clam_id) AS total_claims
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` mx
  LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_hcpcs_codes_mapped` hcpcs
    ON mx.prcr_cd = hcpcs.procedure_code
  WHERE mx.pt_cycle_id = (SELECT MAX(pt_cycle_id) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)
    AND hcpcs.regimen = 'CDK'
    AND mx.svc_dt BETWEEN 
      DATE_TRUNC('MONTH', ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(svc_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)), -13))
    AND 
      LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(svc_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)), -2))
  GROUP BY DATE_TRUNC('month', mx.svc_dt)
)
SELECT 
  COALESCE(rx.month, mx.month) AS month,
  COALESCE(rx.total_claims, 0) + COALESCE(mx.total_claims, 0) AS total_claims
FROM rx_claims rx
FULL OUTER JOIN mx_claims mx ON rx.month = mx.month
ORDER BY month'''
            
            cdk_claims_python="""import pandas as pd
import plotly.graph_objects as go
import pickle
result = result_data.copy()

# MANDATORY: Save raw result as CSV FIRST
csv_filename = "Structured_Bot/output_files/cdk_claims_monthly_trend_data.csv"
result.to_csv(csv_filename, index=False)

# Convert month to datetime if not already
result['month'] = pd.to_datetime(result['month'])

# Create the line chart
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=result['month'],
    y=result['total_claims'],
    mode='lines+markers',
    name='CDK Claims',
    line=dict(color='#1f77b4', width=2),
    marker=dict(size=8)
))

fig.update_layout(
    title='Monthly Trend of CDK Claims Over the Past Year',
    xaxis_title='Month',
    yaxis_title='Total Claims',
    font=dict(size=12),
    hovermode='x unified'
)

# Add percentage change annotations
for i in range(1, len(result)):
    prev_claims = result['total_claims'].iloc[i-1]
    curr_claims = result['total_claims'].iloc[i]
    pct_change = ((curr_claims - prev_claims) / prev_claims) * 100
    
    fig.add_annotation(
        x=result['month'].iloc[i],
        y=curr_claims,
        text=f"{pct_change:.1f}%",
        showarrow=True,
        arrowhead=4,
        arrowsize=0.5,
        arrowwidth=1,
        arrowcolor="#636363",
        ax=0,
        ay=-40
    )

# Save plot as pickle
filename = "Structured_Bot/output_files/cdk_claims_monthly_trend.pkl"
with open(filename, 'wb') as f:
    pickle.dump(fig, f)
"""
            cdk_claims_analysis = f'''"I'll generate the SQL query to fetch the monthly trend of CDK claims over the past year, then visualize the data using Python, and finally provide insights based on the results.
Step 1: Generate SQL Query
Let's start by writing an SQL query to retrieve the monthly trend of CDK claims for the past year."'''

            # Run the code
            if run:
                exec(cdk_claims_python)
            else:
                return cdk_claims_sql,cdk_claims_python,cdk_claims_analysis   
        

        if query.lower() == 'compare the monthly medical claims trend of trodelvy between latest two batches over the past two years?':
            mx_claims_batch_sql = f'''WITH latest_batches AS (
  SELECT DISTINCT pt_cycle_id
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`
  ORDER BY pt_cycle_id DESC
  LIMIT 2
),
max_date AS (
  SELECT MAX(svc_dt) AS max_svc_dt
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`
)
SELECT 
  mx.pt_cycle_id,
  DATE_TRUNC('MONTH', mx.svc_dt) AS month,
  COUNT(mx.mx_clam_id) AS claim_count
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` mx
LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_hcpcs_codes_mapped` hcpcs 
  ON mx.prcr_cd = hcpcs.procedure_code
LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc 
  ON mx.ndc11 = ndc.product_code
CROSS JOIN max_date
WHERE mx.pt_cycle_id IN (SELECT pt_cycle_id FROM latest_batches)
  AND (hcpcs.regimen = 'TRODELVY' OR ndc.regimen = 'TRODELVY')
  AND mx.svc_dt BETWEEN DATE_TRUNC('MONTH', ADD_MONTHS(DATE_TRUNC('MONTH', max_date.max_svc_dt), -25))
                    AND LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', max_date.max_svc_dt), -2))
GROUP BY mx.pt_cycle_id, DATE_TRUNC('MONTH', mx.svc_dt)
ORDER BY mx.pt_cycle_id, month'''
            
            mx_claims_batch_python="""import pandas as pd
import plotly.graph_objects as go
import pickle

result = result_data.copy()

# Convert month to datetime if not already
result['month'] = pd.to_datetime(result['month'])
result.to_csv('Structured_Bot/output_files/trodelvy_claims_trend.csv', index=False)

# Create a pivot table for easier plotting
pivot_data = result.pivot(index='month', columns='pt_cycle_id', values='claim_count')

# Create the Plotly figure
fig = go.Figure()

# Add line traces for each batch
batch_labels = ['Latest Batch', 'Previous Batch']
for i, column in enumerate(pivot_data.columns):
    fig.add_trace(go.Scatter(
        x=pivot_data.index,
        y=pivot_data[column],
        mode='lines+markers',
        name=batch_labels[i] if i < len(batch_labels) else f'Batch {column}',
        line=dict(width=2),
        marker=dict(size=8)
    ))

# Update layout
fig.update_layout(
    title='Monthly Medical Claims Trend of TRODELVY - Latest Two Batches',
    title_font_size=16,
    xaxis_title='Month',
    yaxis_title='Claim Count',
    xaxis_title_font_size=12,
    yaxis_title_font_size=12,
    width=1200,
    height=600,
    hovermode='x unified',
    legend=dict(title='Batch ID'),
    xaxis=dict(tickangle=45),
    plot_bgcolor='white',
    xaxis_showgrid=True,
    yaxis_showgrid=True,
    xaxis_gridcolor='rgba(128,128,128,0.3)',
    yaxis_gridcolor='rgba(128,128,128,0.3)'
)

# Save the Plotly figure as pickle
with open('Structured_Bot/output_files/trodelvy_claims_trend.pkl', 'wb') as f:
    pickle.dump(fig, f)

# Calculate some statistics for insights
latest_batch = pivot_data.iloc[:, 0]
previous_batch = pivot_data.iloc[:, 1]

stats = pd.DataFrame({
    'Latest Batch': latest_batch.describe(),
    'Previous Batch': previous_batch.describe()
})

percent_change = ((latest_batch.mean() - previous_batch.mean()) / previous_batch.mean()) * 100
"""
            mx_claims_batch_analysis = f'''"I'll analyze the monthly medical claims trend for TRODELVY comparing the latest two batches over the past two years, considering a two-month lag. Let's break this down into steps:
Retrieve the data using SQL
Process and visualize the data using Python
Generate insights based on the visualization
Let's start with the SQL query to fetch the required data:"'''

            # Run the code
            if run:
                exec(mx_claims_batch_python)
            else:
                return mx_claims_batch_sql,mx_claims_batch_python,mx_claims_batch_analysis


        if query.lower() == 'analyze the monthly trend of paid rx+ mx claims and flag any outliers?':
            rx_mx_claims_outlier_sql = f'''WITH rx_claims AS (
    SELECT 
        DATE_TRUNC('month', rx.fill_dt) as claim_month,
        COUNT(rx.clam_id) as rx_claims
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` rx
    WHERE rx.pt_cycle_id = (SELECT MAX(pt_cycle_id) 
                            FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)
        AND rx.clam_type = 'PD'
        AND rx.fill_dt <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', 
            (SELECT MAX(fill_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` 
             WHERE pt_cycle_id = (SELECT MAX(pt_cycle_id) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`))), -2))
    GROUP BY claim_month
),
mx_claims AS (
    SELECT 
        DATE_TRUNC('month', mx.svc_dt) as claim_month,
        COUNT(mx.mx_clam_id) as mx_claims
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` mx
    WHERE mx.pt_cycle_id = (SELECT MAX(pt_cycle_id) 
                            FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)
        AND mx.svc_dt <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', 
            (SELECT MAX(svc_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` 
             WHERE pt_cycle_id = (SELECT MAX(pt_cycle_id) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`))), -2))
    GROUP BY claim_month
),
combined_claims AS (
    SELECT 
        COALESCE(rx.claim_month, mx.claim_month) as claim_month,
        COALESCE(rx.rx_claims, 0) + COALESCE(mx.mx_claims, 0) as total_claims
    FROM rx_claims rx
    FULL OUTER JOIN mx_claims mx ON rx.claim_month = mx.claim_month
),
quartiles AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_claims) as q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_claims) as median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_claims) as q3
    FROM combined_claims
),
bounds AS (
    SELECT 
        q1,
        q3,
        median,
        (q3 - q1) as iqr,
        q1 - 1.5 * (q3 - q1) as lower_bound,
        q3 + 1.5 * (q3 - q1) as upper_bound
    FROM quartiles
)
SELECT 
    c.*,
    b.median,
    b.q1,
    b.q3,
    b.iqr,
    b.lower_bound,
    b.upper_bound,
    CASE 
        WHEN c.total_claims > b.upper_bound THEN 'High Outlier'
        WHEN c.total_claims < b.lower_bound THEN 'Low Outlier'
        ELSE 'Normal'
    END as outlier_flag,
    ((c.total_claims - b.median) / NULLIF(b.median, 0)) * 100 as pct_deviation_from_median
FROM combined_claims c
CROSS JOIN bounds b
ORDER BY c.claim_month'''
            
            rx_mx_claims_outlier_python="""import pandas as pd
import plotly.graph_objects as go
import pickle
result = result_data.copy()
# Ensure the claim_month is in datetime format
result['claim_month'] = pd.to_datetime(result['claim_month'])

# Save data to CSV
result.to_csv('Structured_Bot/output_files/monthly_rx_mx_claims_trend.csv', index=False)

# Create the Plotly figure
fig = go.Figure()

# Add main line plot
fig.add_trace(go.Scatter(
    x=result['claim_month'],
    y=result['total_claims'],
    mode='lines+markers',
    name='Total Claims',
    line=dict(color='blue', width=2),
    marker=dict(size=6)
))

# Highlight outliers
outliers = result[result['outlier_flag'] != 'Normal']
if not outliers.empty:
    fig.add_trace(go.Scatter(
        x=outliers['claim_month'],
        y=outliers['total_claims'],
        mode='markers',
        name='Outliers',
        marker=dict(color='red', size=12, symbol='circle')
    ))

# Add reference lines
median_value = result['median'].iloc[0]
upper_bound_value = result['upper_bound'].iloc[0]
lower_bound_value = result['lower_bound'].iloc[0]

fig.add_hline(y=median_value, line_dash="dash", line_color="green", 
              annotation_text="Median", annotation_position="right")
fig.add_hline(y=upper_bound_value, line_dash="dot", line_color="red", 
              annotation_text="Upper Bound", annotation_position="right")
fig.add_hline(y=lower_bound_value, line_dash="dot", line_color="red", 
              annotation_text="Lower Bound", annotation_position="right")

# Update layout
fig.update_layout(
    title='Monthly Trend of Paid Rx + Mx Claims with Outliers',
    title_font_size=16,
    xaxis_title='Month',
    yaxis_title='Total Claims',
    xaxis_title_font_size=12,
    yaxis_title_font_size=12,
    width=1600,
    height=800,
    hovermode='x unified',
    showlegend=True,
    xaxis=dict(tickangle=45),
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis_showgrid=True,
    yaxis_showgrid=True,
    xaxis_gridcolor='rgba(128,128,128,0.3)',
    yaxis_gridcolor='rgba(128,128,128,0.3)'
)

# Save the Plotly figure as a pickle file
with open('Structured_Bot/output_files/monthly_rx_mx_claims_trend.pkl', 'wb') as f:
    pickle.dump(fig, f)

# Generate summary statistics
summary = result.describe()

# Count outliers
outlier_count = result['outlier_flag'].value_counts()

# Top 5 months with highest claims
top_5_high = result.nlargest(5, 'total_claims')[['claim_month', 'total_claims', 'outlier_flag']]

# Top 5 months with lowest claims
top_5_low = result.nsmallest(5, 'total_claims')[['claim_month', 'total_claims', 'outlier_flag']]
"""
            rx_mx_claims_outlier_analysis = f'''"I'll analyze the monthly trend of paid Rx + Mx claims for the entire available data period and flag any outliers using the IQR (Interquartile Range) method. Let's break this down into steps:
First, we'll query the data to get the monthly trend of paid Rx + Mx claims.
Then, we'll apply the IQR method to identify outliers.
Finally, we'll visualize the results and provide insights.
Let's start with the SQL query:"'''

            # Run the code
            if run:
                exec(rx_mx_claims_outlier_python)
            else:
                return rx_mx_claims_outlier_sql,rx_mx_claims_outlier_python,rx_mx_claims_outlier_analysis
            
        if query.lower() == 'compare monthly claims (rx+mx) for tro  between latest two batches and identify outliers':
            rx_mx_claims_batch_sql = f'''WITH rx_batches AS (
    SELECT pt_cycle_id, 
           ROW_NUMBER() OVER (ORDER BY pt_cycle_id DESC) as batch_rank
    FROM (SELECT DISTINCT pt_cycle_id 
          FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)
),
mx_batches AS (
    SELECT pt_cycle_id,
           ROW_NUMBER() OVER (ORDER BY pt_cycle_id DESC) as batch_rank
    FROM (SELECT DISTINCT pt_cycle_id 
          FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)
),
-- BATCH 1 (LATEST) WITH 2-MONTH LAG
rx_batch1 AS (
    SELECT 
        DATE_TRUNC('MONTH', rx.fill_dt) as claim_month,
        COUNT(rx.clam_id) as rx_claims
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` rx
    LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc
        ON rx.ndc11 = ndc.product_code
    WHERE rx.pt_cycle_id = (SELECT pt_cycle_id FROM rx_batches WHERE batch_rank = 1)
        AND rx.clam_type = 'PD'
        AND ndc.regimen = 'TRODELVY'
        AND ndc.product_code IS NOT NULL
        AND rx.fill_dt <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', 
            (SELECT MAX(fill_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` 
             WHERE pt_cycle_id = (SELECT pt_cycle_id FROM rx_batches WHERE batch_rank = 1))), -2))
    GROUP BY claim_month
),
mx_batch1 AS (
    SELECT 
        DATE_TRUNC('MONTH', mx.svc_dt) as claim_month,
        COUNT(mx.mx_clam_id) as mx_claims
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` mx
    LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_hcpcs_codes_mapped` hcpcs
        ON mx.prcr_cd = hcpcs.procedure_code
    LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc
        ON mx.ndc11 = ndc.product_code
    WHERE mx.pt_cycle_id = (SELECT pt_cycle_id FROM mx_batches WHERE batch_rank = 1)
        AND (hcpcs.regimen = 'TRODELVY' OR ndc.regimen = 'TRODELVY')
        AND (hcpcs.procedure_code IS NOT NULL OR ndc.product_code IS NOT NULL)
        AND mx.svc_dt <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', 
            (SELECT MAX(svc_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` 
             WHERE pt_cycle_id = (SELECT pt_cycle_id FROM mx_batches WHERE batch_rank = 1))), -2))
    GROUP BY claim_month
),
batch1_combined AS (
    SELECT 
        COALESCE(rx.claim_month, mx.claim_month) as claim_month,
        COALESCE(rx.rx_claims, 0) + COALESCE(mx.mx_claims, 0) as total_claims,
        1 as batch_number
    FROM rx_batch1 rx
    FULL OUTER JOIN mx_batch1 mx ON rx.claim_month = mx.claim_month
),
-- BATCH 2 (PREVIOUS) WITH 2-MONTH LAG
rx_batch2 AS (
    SELECT 
        DATE_TRUNC('MONTH', rx.fill_dt) as claim_month,
        COUNT(rx.clam_id) as rx_claims
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` rx
    LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc
        ON rx.ndc11 = ndc.product_code
    WHERE rx.pt_cycle_id = (SELECT pt_cycle_id FROM rx_batches WHERE batch_rank = 2)
        AND rx.clam_type = 'PD'
        AND ndc.regimen = 'TRODELVY'
        AND ndc.product_code IS NOT NULL
        AND rx.fill_dt <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', 
            (SELECT MAX(fill_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` 
             WHERE pt_cycle_id = (SELECT pt_cycle_id FROM rx_batches WHERE batch_rank = 2))), -2))
    GROUP BY claim_month
),
mx_batch2 AS (
    SELECT 
        DATE_TRUNC('MONTH', mx.svc_dt) as claim_month,
        COUNT(mx.mx_clam_id) as mx_claims
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` mx
    LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_hcpcs_codes_mapped` hcpcs
        ON mx.prcr_cd = hcpcs.procedure_code
    LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc
        ON mx.ndc11 = ndc.product_code
    WHERE mx.pt_cycle_id = (SELECT pt_cycle_id FROM mx_batches WHERE batch_rank = 2)
        AND (hcpcs.regimen = 'TRODELVY' OR ndc.regimen = 'TRODELVY')
        AND (hcpcs.procedure_code IS NOT NULL OR ndc.product_code IS NOT NULL)
        AND mx.svc_dt <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', 
            (SELECT MAX(svc_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` 
             WHERE pt_cycle_id = (SELECT pt_cycle_id FROM mx_batches WHERE batch_rank = 2))), -2))
    GROUP BY claim_month
),
batch2_combined AS (
    SELECT 
        COALESCE(rx.claim_month, mx.claim_month) as claim_month,
        COALESCE(rx.rx_claims, 0) + COALESCE(mx.mx_claims, 0) as total_claims,
        2 as batch_number
    FROM rx_batch2 rx
    FULL OUTER JOIN mx_batch2 mx ON rx.claim_month = mx.claim_month
),
batch_comparison AS (
    SELECT 
        b1.claim_month,
        b1.total_claims as batch1_claims,
        b2.total_claims as batch2_claims,
        b1.total_claims - b2.total_claims as absolute_change,
        CASE WHEN b2.total_claims > 0 
            THEN ((b1.total_claims - b2.total_claims)::FLOAT / b2.total_claims) * 100
            ELSE NULL 
        END as pct_change
    FROM batch1_combined b1
    FULL OUTER JOIN batch2_combined b2 ON b1.claim_month = b2.claim_month
),
pct_change_quartiles AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pct_change) as q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pct_change) as median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pct_change) as q3
    FROM batch_comparison
    WHERE pct_change IS NOT NULL
),
pct_change_bounds AS (
    SELECT 
        q1,
        q3,
        median,
        (q3 - q1) as iqr,
        q1 - 1.5 * (q3 - q1) as lower_bound,
        q3 + 1.5 * (q3 - q1) as upper_bound
    FROM pct_change_quartiles
)
SELECT 
    bc.claim_month,
    bc.batch1_claims,
    bc.batch2_claims,
    bc.absolute_change,
    bc.pct_change,
    b.median as median_pct_change,
    b.q1,
    b.q3,
    b.iqr,
    b.lower_bound,
    b.upper_bound,
    CASE 
        WHEN bc.pct_change > b.upper_bound THEN 'High Outlier'
        WHEN bc.pct_change < b.lower_bound THEN 'Low Outlier'
        ELSE 'Normal'
    END as outlier_flag,
    ((bc.pct_change - b.median) / NULLIF(b.median, 0)) * 100 as deviation_from_median_pct
FROM batch_comparison bc
CROSS JOIN pct_change_bounds b
WHERE bc.pct_change IS NOT NULL
ORDER BY 
    CASE 
        WHEN bc.pct_change > b.upper_bound THEN bc.pct_change - b.upper_bound
        WHEN bc.pct_change < b.lower_bound THEN b.lower_bound - bc.pct_change
        ELSE 0
    END DESC'''
            
            rx_mx_claims_batch_python="""import pandas as pd
import plotly.graph_objects as go
import pickle
result = result_data.copy()
# MANDATORY: Save raw result as CSV FIRST before any processing
csv_filename = "Structured_Bot/output_files/trodelvy_monthly_claims_comparison_data.csv"
result.to_csv(csv_filename, index=False)

# Data validation and cleaning
result = result.copy()
result['claim_month'] = pd.to_datetime(result['claim_month'])
result['pct_change'] = pd.to_numeric(result['pct_change'], errors='coerce')
result['deviation_from_median_pct'] = pd.to_numeric(result['deviation_from_median_pct'], errors='coerce')

# Create the plot
fig = go.Figure()

# Add bar chart for percentage change
fig.add_trace(go.Bar(
    x=result['claim_month'],
    y=result['pct_change'],
    name='Percentage Change',
    marker_color=['red' if flag == 'High Outlier' else 'green' if flag == 'Low Outlier' else 'blue' for flag in result['outlier_flag']]
))

# Add lines for upper and lower bounds
fig.add_trace(go.Scatter(
    x=result['claim_month'],
    y=[result['upper_bound'].iloc[0]] * len(result),
    mode='lines',
    name='Upper Bound',
    line=dict(color='red', dash='dash')
))

fig.add_trace(go.Scatter(
    x=result['claim_month'],
    y=[result['lower_bound'].iloc[0]] * len(result),
    mode='lines',
    name='Lower Bound',
    line=dict(color='green', dash='dash')
))

# Add line for median
fig.add_trace(go.Scatter(
    x=result['claim_month'],
    y=[result['median_pct_change'].iloc[0]] * len(result),
    mode='lines',
    name='Median',
    line=dict(color='black', dash='dot')
))

# Update layout
fig.update_layout(
    title='Monthly TRODELVY Claims Comparison (Batch 1 vs Batch 2)',
    xaxis_title='Claim Month',
    yaxis_title='Percentage Change (%)',
    showlegend=True
)

# Save plot as pickle
filename = "Structured_Bot/output_files/trodelvy_monthly_claims_comparison_plot.pkl"
with open(filename, 'wb') as f:
    pickle.dump(fig, f)
"""
            rx_mx_claims_batch_analysis = f'''" I'll compare the monthly claims (RX and MX combined) for TRODELVY between the latest two batches and identify outliers using the IQR method. Let's break this down step by step.
First, we'll query the data for TRODELVY claims from both RX and MX tables for the latest two batches.
Then, we'll combine the data and calculate the percentage change between batches.
Finally, we'll apply the IQR method to identify outliers in the percentage changes.
"'''

            # Run the code
            if run:
                exec(rx_mx_claims_batch_python)
            else:
                return rx_mx_claims_batch_sql,rx_mx_claims_batch_python,rx_mx_claims_batch_analysis
            
        if query.lower() == 'identify months with significant outliers in targeted therapies claims':
            targeted_therapy_sql = f'''WITH combined_claims AS (
    -- RX Claims
    SELECT
        DATE_TRUNC('MONTH', rx.fill_dt) AS claim_month,
        COUNT(rx.clam_id) AS claim_count,
        'RX' AS claim_type
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed` rx
    LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc
        ON rx.ndc11 = ndc.product_code
    WHERE rx.clam_type = 'PD'
    AND rx.pt_cycle_id = (SELECT MAX(pt_cycle_id) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)
    AND ndc.regimen IN ('ITOVEBI', 'CDK', 'HER2 POSITIVE DRUGS', 'DATROWAY', 'ENHERTU', 'TRODELVY', 'OTHER TARGETED', 'PD-1/PD-L1 BASED')
    AND rx.fill_dt <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(fill_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_rx_onc_hist_preprocessed`)), -2))
    GROUP BY claim_month

    UNION ALL

    -- MX Claims
    SELECT
        DATE_TRUNC('MONTH', mx.svc_dt) AS claim_month,
        COUNT(mx.mx_clam_id) AS claim_count,
        'MX' AS claim_type
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed` mx
    LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_hcpcs_codes_mapped` hcpcs
        ON mx.prcr_cd = hcpcs.procedure_code
    LEFT JOIN `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`breast_cancer_drugs_ndc_codes_mapped` ndc
        ON mx.ndc11 = ndc.product_code
    WHERE mx.pt_cycle_id = (SELECT MAX(pt_cycle_id) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)
    AND (hcpcs.regimen IN ('ITOVEBI', 'CDK', 'HER2 POSITIVE DRUGS', 'DATROWAY', 'ENHERTU', 'TRODELVY', 'OTHER TARGETED', 'PD-1/PD-L1 BASED')
         OR ndc.regimen IN ('ITOVEBI', 'CDK', 'HER2 POSITIVE DRUGS', 'DATROWAY', 'ENHERTU', 'TRODELVY', 'OTHER TARGETED', 'PD-1/PD-L1 BASED'))
    AND mx.svc_dt <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', (SELECT MAX(svc_dt) FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`stg_iqvia_laad_mx_onc_hist_preprocessed`)), -2))
    GROUP BY claim_month
),
monthly_totals AS (
    SELECT
        claim_month,
        SUM(claim_count) AS total_claims
    FROM combined_claims
    GROUP BY claim_month
),
quartiles AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_claims) AS q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_claims) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_claims) AS q3
    FROM monthly_totals
),
bounds AS (
    SELECT
        q1,
        q3,
        median,
        (q3 - q1) AS iqr,
        q1 - 1.5 * (q3 - q1) AS lower_bound,
        q3 + 1.5 * (q3 - q1) AS upper_bound
    FROM quartiles
)
SELECT 
    mt.*,
    b.median,
    b.q1,
    b.q3,
    b.iqr,
    b.lower_bound,
    b.upper_bound,
    CASE 
        WHEN mt.total_claims > b.upper_bound THEN 'High Outlier'
        WHEN mt.total_claims < b.lower_bound THEN 'Low Outlier'
        ELSE 'Normal'
    END AS outlier_flag,
    ((mt.total_claims - b.median) / NULLIF(b.median, 0)) * 100 AS pct_deviation_from_median
FROM monthly_totals mt
CROSS JOIN bounds b
ORDER BY 
    CASE 
        WHEN mt.total_claims > b.upper_bound THEN mt.total_claims - b.upper_bound
        WHEN mt.total_claims < b.lower_bound THEN b.lower_bound - mt.total_claims
        ELSE 0
    END DESC'''
            
            targeted_therapy_python="""import pandas as pd
import plotly.graph_objects as go
import pickle
result = result_data.copy()
# MANDATORY: Save raw result as CSV FIRST before any processing
csv_filename = "Structured_Bot/output_files/targeted_therapies_claims_outliers_data.csv"
result.to_csv(csv_filename, index=False)

# Convert claim_month to datetime if it's not already
result['claim_month'] = pd.to_datetime(result['claim_month'])

# Sort the dataframe by claim_month
result = result.sort_values('claim_month')

# Create the plot
fig = go.Figure()

# Add the main line for total claims
fig.add_trace(go.Scatter(
    x=result['claim_month'],
    y=result['total_claims'],
    mode='lines+markers',
    name='Total Claims',
    line=dict(color='blue', width=2),
    marker=dict(size=8)
))

# Add upper and lower bounds
fig.add_trace(go.Scatter(
    x=result['claim_month'],
    y=result['upper_bound'],
    mode='lines',
    name='Upper Bound',
    line=dict(color='red', width=1, dash='dash')
))

fig.add_trace(go.Scatter(
    x=result['claim_month'],
    y=result['lower_bound'],
    mode='lines',
    name='Lower Bound',
    line=dict(color='red', width=1, dash='dash')
))

# Add median line
fig.add_trace(go.Scatter(
    x=result['claim_month'],
    y=result['median'],
    mode='lines',
    name='Median',
    line=dict(color='green', width=1, dash='dot')
))

# Highlight outliers
outliers = result[result['outlier_flag'] != 'Normal']
fig.add_trace(go.Scatter(
    x=outliers['claim_month'],
    y=outliers['total_claims'],
    mode='markers',
    name='Outliers',
    marker=dict(color='red', size=12, symbol='star')
))

# Update layout
fig.update_layout(
    title='Targeted Therapies Claims Over Time with Outliers',
    xaxis_title='Claim Month',
    yaxis_title='Total Claims',
    legend_title='Legend',
    font=dict(size=12),
    hovermode='x unified'
)

# Save plot as pickle
plot_filename = "Structured_Bot/output_files/targeted_therapies_claims_outliers_plot.pkl"
with open(plot_filename, 'wb') as f:
    pickle.dump(fig, f)


# Print outlier information
outliers = result[result['outlier_flag'] != 'Normal']
"""
            targeted_therapy_analysis = f'''" I'll analyze the targeted therapies claims over the entire data period, excluding the last two months for data completeness, and identify months with significant outliers using the IQR method. Let's break this down step by step.
Step 1: Retrieve the data
First, we'll query the data for targeted therapies claims, applying the necessary filters and excluding the last two months."'''

            # Run the code
            if run:
                exec(targeted_therapy_python)
            else:
                return targeted_therapy_sql,targeted_therapy_python,targeted_therapy_analysis

        if query.lower() == 'what is the nps share by regimen group in lot 2 and tier 1 hcps in tnbc?':
            tier1_nps_sql_code = f'''WITH Max_Date AS (
  SELECT MAX(CAST(Year_Month AS INT)) AS max_date 
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`
  WHERE LOWER(TRIM(indication)) = LOWER('tnbc')
    AND TRIM(Line_of_Therapy_LOT) = '2'
    AND LOWER(TRIM(NPI_HCP_tier)) = 'tier 1'
    AND new_patient_flag = '1'
),
Regimen_Data AS (
  SELECT 
    regimen_grouped_name,
    Year_Month,
    SUM(Monthly_Adjusted_Count) AS nps_count
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`
  WHERE TRIM(Line_of_Therapy_LOT) = '2'
    AND LOWER(TRIM(NPI_HCP_tier)) = 'tier 1'
    AND LOWER(TRIM(indication)) = LOWER('tnbc')
    AND new_patient_flag = '1'
    AND CAST(Year_Month AS INT) = (SELECT max_date FROM Max_Date)
  GROUP BY regimen_grouped_name, Year_Month
)
SELECT 
  regimen_grouped_name,
  Year_Month,
  nps_count,
  nps_count / SUM(nps_count) OVER () AS nps_share
FROM Regimen_Data
ORDER BY nps_share DESC;'''
            
            tier1_nps_python_code="""import os
import matplotlib.pyplot as plt

# Ensure output directory exists
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)

# The SQL query already calculates the share, so we can use it directly
# Convert nps_share from decimal to percentage
result_data['Share'] = result_data['nps_share'] * 100

# Keep only required columns and rename for consistency
regimen_col = 'regimen_grouped_name'
share_col = 'Share'
grouped_data_share_df = result_data[[regimen_col, share_col]].copy()
grouped_data_share_df.rename(columns={regimen_col: 'Regimen'}, inplace=True)

# Plot
plt.figure(figsize=(12, 8))
plt.bar(grouped_data_share_df['Regimen'], grouped_data_share_df['Share'], color='skyblue')
plt.title('NPS Share by Regimen Group in LOT 2 and Tier 1 HCPs in TNBC')
plt.xlabel('Regimen Grouped Name')
plt.ylabel('Share (%)')
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Define file paths
csv_output_path = os.path.join(output_dir, csv_filename)
img_output_path = os.path.join(output_dir, chart_name)

# Save output files
plt.savefig(img_output_path, dpi=300, bbox_inches='tight')
grouped_data_share_df.to_csv(csv_output_path, index=False)

# Confirmation
print("Saved CSV: " + str(csv_output_path))
print("Saved Image: " + str(img_output_path))

# Display data for verification
print("Data Summary:")
display_cols = [regimen_col, 'nps_count', share_col]
print(result_data[display_cols])"""
            tier1_nps_analysis = f'''To answer the query about the New Patient Start (NPS) share by regimen group for Line of Therapy (LOT) 2 and Tier 1 HCPs in {"tnbc"}, we can break down the task into the following sub-tasks:\n
\n
1. **Data Source**: We will use the claims_data Dask dataframe.\n
2. **Filter Data**: Filter the data for patients in LOT 2, Tier 1 HCPs, and {"tnbc"} indication.\n
3. **Further Filter for New Patients**: From the filtered data, select only new patients.\n
4. **Group and Aggregate**: Group the data by 'regimen_grouped_name' and sum the 'Monthly_Adjusted_Count' for each group.\n
5. **Calculate Total NPS**: Calculate the total 'Monthly_Adjusted_Count' for all regimens combined.\n
6. **Calculate Share**: For each regimen, calculate the share as the ratio of the regimen's 'Monthly_Adjusted_Count' to the total 'Monthly_Adjusted_Count'.\n
7. **Visualization**: Plot the shares as a bar chart and save the data and chart.\n
Let's start with the first sub-task of filtering the data.'''

            # Run the code
            if run:
                exec(tier1_nps_python_code)
            else:
                return tier1_nps_sql_code,tier1_nps_python_code,tier1_nps_analysis
        if query.lower()=='in lot 2, how is nps share trending by month by regimen group for tnbc?':
            monthly_nps_share_sql_code = f'''SELECT
    Year_Month,
    regimen_grouped_name AS Regimen,
    SUM(Monthly_Adjusted_Count) AS Monthly_NPS
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`
WHERE
    TRIM(Line_of_Therapy_LOT) = '2'
    AND new_patient_flag = '1'
    AND LOWER(TRIM(indication)) = LOWER('{"tnbc"}')
GROUP BY
    Year_Month,
    regimen_grouped_name'''
            monthly_nps_share_python_code=f'''import os
import matplotlib.pyplot as plt
# Define output directory and ensure it exists
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)
# result_data must contain columns: ['Year_Month', 'Regimen', 'Monthly_NPS']
monthly_grouped_df = result_data.copy()

# Step 2: Compute total NPS per month
total_nps_per_month = monthly_grouped_df.groupby('Year_Month')['Monthly_NPS'].sum().reset_index()
total_nps_per_month.columns = ['Year_Month', 'Total_Monthly_NPS']

# Step 3: Merge and calculate share
monthly_grouped_df = monthly_grouped_df.merge(total_nps_per_month, on='Year_Month')
monthly_grouped_df['Share'] = monthly_grouped_df['Monthly_NPS'] / monthly_grouped_df['Total_Monthly_NPS']

# Step 4: Pivot table for stacked bar chart
pivot_df = monthly_grouped_df.pivot(index='Year_Month', columns='Regimen', values='Share')

# Step 5: Plot
fig, ax = plt.subplots(figsize=(12, 8))
pivot_df.plot(kind='bar', stacked=True, ax=ax)
ax.set_title(f'NPS Share Trend by Month by Regimen Group for {"tnbc"} in LOT 2')
ax.set_xlabel('Year_Month')
ax.set_ylabel('Share')
plt.xticks(rotation=45)
plt.legend(title='Regimen Group', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()

# Step 6: Save outputs
csv_output_path = os.path.join(output_dir, csv_filename)
img_output_path = os.path.join(output_dir, chart_name)

plt.savefig(img_output_path, dpi=300, bbox_inches='tight')
pivot_df.to_csv(csv_output_path)

# Step 7: Confirmation
print(f"Saved CSV: csv_output_path")
print(f"Saved Image: img_output_path")

            '''
            monthly_share_analysis = f'''To answer the query about how the New Patient Start (NPS) share is trending by month by regimen group for {"tnbc"} in Line of Therapy (LOT) 2, we can break down the task into the following sub-tasks:

1. **Data Source**: We will use the claims_data Dask dataframe.
2. **Filter Data**: Filter the data for patients in LOT 2 and for the indication {"tnbc"}.
3. **Further Filter for New Patients**: Filter the data to include only new patients (new_patient_flag = '1').
4. **Group Data**: Group the data by 'Year_Month' and 'regimen_grouped_name'.
5. **Calculate Monthly NPS Share**: Calculate the share of each regimen group by month.
6. **Plot the Trend**: Plot the trend of NPS share by month for each regimen group.'''
            if run:
                exec(monthly_nps_share_python_code)
            else:
                return monthly_nps_share_sql_code,monthly_nps_share_python_code, monthly_share_analysis
        if query.lower()=='what is the source of business share contributing to trodelvy in lot 2 during q2 2024 in tnbc?':

            sob_sql_code = f'''WITH filtered_data AS (
  SELECT 
    source_of_business,
    SUM(Monthly_Adjusted_Count) AS patient_count
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`
  WHERE LOWER(indication) ILIKE 'tnbc'
    AND Line_of_Therapy_LOT = '2'
    AND Year = '2024'
    AND Quarter = '2'
    AND LOWER(regimen_grouped_name) ILIKE '%trodelvy%'
    AND new_patient_flag = '1'
  GROUP BY source_of_business
),
total_count AS (
  SELECT SUM(patient_count) AS total_patients
  FROM filtered_data
)
SELECT 
  fd.source_of_business,
  fd.patient_count,
  (fd.patient_count / tc.total_patients) * 100 AS share_percentage
FROM filtered_data fd
CROSS JOIN total_count tc
ORDER BY share_percentage DESC'''
            sob_python_code=f'''import os 
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)
grouped_data_computed = result_data.set_index('source_of_business')['patient_count']
total_count = grouped_data_computed.sum()
shares = grouped_data_computed / total_count

# Save CSV
output_csv_path = os.path.join(output_dir, csv_filename)
shares.to_csv(output_csv_path, index=True)

import pandas as pd
import matplotlib.pyplot as plt

# Assuming the SQL result is stored in the 'result' DataFrame
# Data validation and cleaning
result = result_data.copy()
result['share_percentage'] = pd.to_numeric(result['share_percentage'], errors='coerce')

# Remove rows with None or NaN values
result = result.dropna(subset=['source_of_business', 'share_percentage'])

# Create a pie chart
plt.figure(figsize=(10, 8))
plt.pie(result['share_percentage'], labels=result['source_of_business'], autopct='%1.1f%%', startangle=90)
plt.title('Source of Business Share for Trodelvy in LOT 2 (Q2 2024, TNBC)')
plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle

# Save the plot as a PNG file
plt.savefig(f'Structured_Bot/output_files/{chart_name}', dpi=300, bbox_inches='tight')

# Save the sales data to a CSV file
result_data.to_csv(f'Structured_Bot/output_files/{csv_filename}', index=False)

print("Analysis completed")'''
            sob_analysis = f'''To answer the query about the source of business share contributing to Trodelvy in Line of Therapy (LOT) 2 during Q2 2024 in the {"tnbc"} indication, we can break down the task into the following sub-tasks:

1. **Data Source**: We will use the claims_data Dask dataframe.
2. **Filter Data**: Filter the data for the drug 'Trodelvy', LOT '2', quarter '2', year '2024', and indication {"tnbc"}.
3. **Group and Aggregate**: Group the data by 'source_of_business' and calculate the sum of 'Monthly_Adjusted_Count' for each group.
4. **Calculate Share**: Calculate the share of each source of business by dividing each group's sum by the total sum of 'Monthly_Adjusted_Count' for all groups.
5. **Visualization**: Plot the shares as a pie chart to visualize the contribution of each source of business.
6. **Output**: Save the resulting data and chart for review.'''
            if run:
                print("Hello C3PO while executing")
                exec(sob_python_code)
            else:
                print("Hello C3PO before return")
                return sob_sql_code, sob_python_code,sob_analysis
            

        print("in refresh file!!!!!!!!!!")
        print("Hello C3PO - 1")
        if query.lower() == 'what is the nps share by regimen group in lot 2 and tier 1 hcps in hr+/her2-?':
            tier1_nps_sql_code = f'''SELECT
    regimen_grouped_name AS Regimen,
    SUM(Monthly_Adjusted_Count) AS Total_Monthly_Adjusted_Count
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`
WHERE
    TRIM(Line_of_Therapy_LOT) = '2'
    AND LOWER(TRIM(NPI_HCP_tier)) = 'tier 1'
    AND LOWER(TRIM(indication)) = LOWER('{"hr+/her2-"}')
    AND new_patient_flag = '1'
GROUP BY
    regimen_grouped_name'''
            
            tier1_nps_python_code=f'''import os
import matplotlib.pyplot as plt
# Ensure output directory exists
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)
# Assuming result_data has columns: 'Regimen', 'Total_Monthly_Adjusted_Count'
total_nps = result_data['Total_Monthly_Adjusted_Count'].sum()
result_data['Share'] = (result_data['Total_Monthly_Adjusted_Count'] / total_nps) * 100

# Keep only required columns
grouped_data_share_df = result_data[['Regimen', 'Share']]

# Plot
plt.figure(figsize=(12, 8))
plt.pie(grouped_data_share_df['Share'], 
         labels=grouped_data_share_df['Regimen'], 
         autopct='%1.1f%%',
         startangle=90,
         colors=plt.cm.Set3.colors)
plt.title(f'NPS Share by Regimen Group in LOT 2 and Tier 1 HCPs in {"hr+/her2-"}')
plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
plt.tight_layout()
plt.show()

# Define file paths
csv_output_path = os.path.join(output_dir, csv_filename)
img_output_path = os.path.join(output_dir, chart_name)

# Save output files
plt.savefig(img_output_path, dpi=300, bbox_inches='tight')
grouped_data_share_df.to_csv(csv_output_path, index=False)

# Confirmation
print(f"Saved CSV: csv_output_path")
print(f"Saved Image: img_output_path ")

            '''
            tier1_nps_analysis = f'''To answer the query about the New Patient Start (NPS) share by regimen group for Line of Therapy (LOT) 2 and Tier 1 HCPs in {"hr+/her2-"}, we can break down the task into the following sub-tasks:\n
\n
1. **Data Source**: We will use the claims_data Dask dataframe.\n
2. **Filter Data**: Filter the data for patients in LOT 2, Tier 1 HCPs, and {"hr+/her2-"} indication.\n
3. **Further Filter for New Patients**: From the filtered data, select only new patients.\n
4. **Group and Aggregate**: Group the data by 'regimen_grouped_name' and sum the 'Monthly_Adjusted_Count' for each group.\n
5. **Calculate Total NPS**: Calculate the total 'Monthly_Adjusted_Count' for all regimens combined.\n
6. **Calculate Share**: For each regimen, calculate the share as the ratio of the regimen's 'Monthly_Adjusted_Count' to the total 'Monthly_Adjusted_Count'.\n
7. **Visualization**: Plot the shares as a bar chart and save the data and chart.\n
Let's start with the first sub-task of filtering the data.'''

            # Run the code
            if run:
                exec(tier1_nps_python_code)
            else:
                return tier1_nps_sql_code,tier1_nps_python_code,tier1_nps_analysis
        if query.lower() ==  'in lot 2, how is nps share trending by month by regimen group for hr+/her2-?':
            monthly_nps_share_sql_code = f'''SELECT
    Year_Month,
    regimen_grouped_name AS Regimen,
    SUM(Monthly_Adjusted_Count) AS Monthly_NPS
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`
WHERE
    TRIM(Line_of_Therapy_LOT) = '2'
    AND new_patient_flag = '1'
    AND LOWER(TRIM(indication)) = LOWER('{"hr+/her2-"}')
GROUP BY
    Year_Month,
    regimen_grouped_name'''
            monthly_nps_share_python_code=f'''import os
import matplotlib.pyplot as plt
# Define output directory and ensure it exists
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)
# result_data must contain columns: ['Year_Month', 'Regimen', 'Monthly_NPS']
monthly_grouped_df = result_data.copy()

# Step 2: Compute total NPS per month
total_nps_per_month = monthly_grouped_df.groupby('Year_Month')['Monthly_NPS'].sum().reset_index()
total_nps_per_month.columns = ['Year_Month', 'Total_Monthly_NPS']

# Step 3: Merge and calculate share
monthly_grouped_df = monthly_grouped_df.merge(total_nps_per_month, on='Year_Month')
monthly_grouped_df['Share'] = monthly_grouped_df['Monthly_NPS'] / monthly_grouped_df['Total_Monthly_NPS']

# Step 4: Pivot table for stacked bar chart
pivot_df = monthly_grouped_df.pivot(index='Year_Month', columns='Regimen', values='Share')

# Step 5: Plot
fig, ax = plt.subplots(figsize=(12, 8))
pivot_df.plot(kind='bar', stacked=True, ax=ax)
ax.set_title(f'NPS Share Trend by Month by Regimen Group for {"hr+/her2-"} in LOT 2')
ax.set_xlabel('Year_Month')
ax.set_ylabel('Share')
plt.xticks(rotation=45)
plt.legend(title='Regimen Group', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()

# Step 6: Save outputs
csv_output_path = os.path.join(output_dir, csv_filename)
img_output_path = os.path.join(output_dir, chart_name)

plt.savefig(img_output_path, dpi=300, bbox_inches='tight')
pivot_df.to_csv(csv_output_path)

# Step 7: Confirmation
print(f"Saved CSV: csv_output_path")
print(f"Saved Image: img_output_path")

            '''
            monthly_share_analysis = f'''To answer the query about how the New Patient Start (NPS) share is trending by month by regimen group for {"hr+/her2-"} in Line of Therapy (LOT) 2, we can break down the task into the following sub-tasks:

1. **Data Source**: We will use the claims_data Dask dataframe.
2. **Filter Data**: Filter the data for patients in LOT 2 and for the indication {"hr+/her2-"}.
3. **Further Filter for New Patients**: Filter the data to include only new patients (new_patient_flag = '1').
4. **Group Data**: Group the data by 'Year_Month' and 'regimen_grouped_name'.
5. **Calculate Monthly NPS Share**: Calculate the share of each regimen group by month.
6. **Plot the Trend**: Plot the trend of NPS share by month for each regimen group.'''
            if run:
                exec(monthly_nps_share_python_code)
            else:
                return monthly_nps_share_sql_code,monthly_nps_share_python_code, monthly_share_analysis
        
        if query.lower() ==  'what is the source of business share contributing to trodelvy in lot 2 during q2 2024 in hr+/her2-?':

            sob_sql_code = f'''SELECT
    source_of_business,
    COUNT(*) AS nps_count
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`
WHERE
    LOWER(regimen_grouped_name) LIKE '%trodelvy%'
    AND Line_of_Therapy_LOT = '2'
    AND Quarter = '2'
    AND Year = '2024'
    AND LOWER(TRIM(indication)) = LOWER('{"hr+/her2-"}')
    AND new_patient_flag = '1'
GROUP BY
    source_of_business'''
            sob_python_code='''import os
import matplotlib.pyplot as plt

# Create output directory
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)

# Process data for pie chart
grouped_data_computed = result_data.set_index('source_of_business')['nps_count']
total_count = grouped_data_computed.sum()
shares = grouped_data_computed / total_count

# Save CSV
output_csv_path = os.path.join(output_dir, csv_filename)
shares.to_csv(output_csv_path, index=True)

# Generate Pie Chart
plt.figure(figsize=(12, 10))
labels = shares.index
wedges, texts, autotexts = plt.pie(
    shares,
    labels=labels,
    autopct='%1.1f%%',
    startangle=90,
    colors=plt.cm.Paired(range(len(shares)))
)

# Add legend
plt.legend(wedges, labels, title="Source of Business", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

# Ensure pie chart is circular
plt.axis('equal')

# Add title
plt.title(f"Source of Business Share for Trodelvy in LOT 2 during Q2 2024 in HR+/HER2-", 
          fontsize=14, fontweight='bold', pad=20)

# Remove y-axis label
plt.ylabel('')

# Save image
output_img_path = os.path.join(output_dir, chart_name)
plt.savefig(output_img_path, bbox_inches='tight', dpi=300)

# Show the plot (optional)
plt.tight_layout()
plt.show()

# Confirmation messages
print(f"Saved CSV: {output_csv_path}")
print(f"Saved Image: {output_img_path}")
print(f"Total records processed: {total_count}")
print(f"Number of categories: {len(shares)}")'''
            sob_analysis = f'''To answer the query about the source of business share contributing to Trodelvy in Line of Therapy (LOT) 2 during Q2 2024 in the {"hr+/her2-"} indication, we can break down the task into the following sub-tasks:

1. **Data Source**: We will use the claims_data Dask dataframe.
2. **Filter Data**: Filter the data for the drug 'Trodelvy', LOT '2', quarter '2', year '2024', and indication {"hr+/her2-"}.
3. **Group and Aggregate**: Group the data by 'source_of_business' and calculate the sum of 'Monthly_Adjusted_Count' for each group.
4. **Calculate Share**: Calculate the share of each source of business by dividing each group's sum by the total sum of 'Monthly_Adjusted_Count' for all groups.
5. **Visualization**: Plot the shares as a pie chart to visualize the contribution of each source of business.
6. **Output**: Save the resulting data and chart for review.'''
            if run:
                print("Hello C3PO while executing")
                exec(sob_python_code)
            else:
                print("Hello C3PO before return")
                return sob_sql_code, sob_python_code,sob_analysis



# class run_sales_clickable_questions:
#     def __init__(self):
#         pass
#     def run_sales_clickable(self, result_data,query, run , chart_name, csv_filename): # whole_data
#         #tro_sales_data=tro_sales.copy()
#         #ddd_sales_data = ddd_sales.rename(columns={'trx_cnt': 'qty_sold_pu'}).copy()
        
#         print("in refresh file!!!!!!!!!!")

#         # tro_sales_data[['area_id',
#         # 'area_nm'
#         # ]] = tro_sales_data[['area_id',
#         # 'area_nm'
#         # ]].astype(str)


#         # ddd_sales_data[['Child_eid','area_id',
#         # 'area_nm'
#         # ]] = ddd_sales_data[['Child_eid','area_id',
#         # 'area_nm'
#         # ]].astype(str)

        
        if query.lower() == 'how are trodelvy monthly sales trending since the 2020 launch?': 
            query1_sql_code =f"""WITH monthly_sales AS (
  SELECT
    DATE_FORMAT(week_end_date, 'yyyy-MM') AS month,
    SUM(qty_sold_pu) AS total_sales
  FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
  WHERE week_end_date >= '2020-01-01'
  GROUP BY DATE_FORMAT(week_end_date, 'yyyy-MM')
)
SELECT
  month,
  total_sales
FROM monthly_sales
ORDER BY month;"""
            query1_python_code=f'''fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(result_data['month'], 
result_data['total_sales'], 
marker='o', linestyle='-', color='b', label='Quantity Sold')

# Set title and labels
ax.set_title('Monthly Sales Trend of Trodelvy Since 2020', fontsize=16)
ax.set_xlabel('Year-Month', fontsize=14)
ax.set_ylabel('Quantity Sold', fontsize=14)

# Rotate x-axis labels for better visibility
plt.xticks(rotation=45)

# Add grid for better readability
plt.grid(visible=True, linestyle='--', alpha=0.6)

# Add legend
ax.legend()

# Save the plot as a PNG file
plt.savefig(f'Structured_Bot/output_files/{chart_name}', dpi=300, bbox_inches='tight')

# Save the sales data to a CSV file
result_data.to_csv(f'Structured_Bot/output_files/{csv_filename}', index=False)'''

            

            query1_analysis = f'''
            To analyze how Trodelvy monthly sales have been trending since its launch in 2020, we can break down the task into the following sub-tasks:
            1.**Data Source:** We will use the 'Trodelvy Sales Data' Dask dataframe.
            2.**Filter Data:** Select records where 'year' is from 2020 onwards.
            3.**Aggregate Data:** Group the data by 'year' and 'month', and sum up the 'qty_sold_pu' to get the total quantity sold per month.
            4.**Visualization:** Plot the aggregated data to show the trend of monthly sales.
            Let's start by performing the sub-tasks. 
            '''

            # Run the code
            if run:
                exec(query1_python_code)
            else:
                return query1_sql_code,query1_python_code, query1_analysis

        if query.lower() == 'how are weekly sales trending in 2024 for the drug trodelvy?':
            query3_sql_code=f'''WITH latest_date_cte AS (
SELECT MAX(week_end_date) AS latest_date
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
),
previous_week_cte AS (
SELECT MAX(week_end_date) AS previous_week_date
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
WHERE week_end_date < (SELECT latest_date FROM latest_date_cte)
),
filtered_data AS (
SELECT 
    week_end_date,
    qty_sold_pu
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
WHERE EXTRACT(YEAR FROM week_end_date) = 2024
AND week_end_date <= (SELECT previous_week_date FROM previous_week_cte)
)
SELECT 
week_end_date,
SUM(qty_sold_pu) AS total_qty_sold
FROM filtered_data
GROUP BY week_end_date
ORDER BY week_end_date ASC;
                '''
            query3_python_code = f'''# Create the plot for the weekly sales trend
plt.figure(figsize=(10, 5))
plt.plot(result_data['week_end_date'], 
        result_data['total_qty_sold'], 
        marker='o', linestyle='-', color='b')

# Set plot title and labels
plt.title('Weekly Sales Trend for Trodelvy in 2024', fontsize=16)
plt.xlabel('Week End Date', fontsize=14)
plt.ylabel('Quantity Sold', fontsize=14)

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Add grid for better readability
plt.grid(True)

# Adjust layout to ensure everything fits
plt.tight_layout()

# Save the plot as a PNG file
plt.savefig(f'Structured_Bot/output_files/{chart_name}', dpi=300, bbox_inches='tight')

# Save the sales data to a CSV file
result_data.to_csv(f'Structured_Bot/output_files/{csv_filename}', index=False)
'''

            query3_analysis = f'''
            To analyze the weekly sales trend for the drug Trodelvy in the year 2024, we can break down the task into the following sub-tasks:
            1.**Data Source:** We will use the 'Trodelvy Sales Data' Dask dataframe.
            2.**Filter Data:** Select records of 'Trodelvy' with the year 2024.
            3.**Aggregate Data:** Group the data by 'week_end_date' and sum the 'qty_sold_pu' to get total sales per week.
            4.**Visualization:**  Sort the data by 'week_end_date' and plot a line chart to visualize the trend.
            Let's start by performing the sub-tasks. '''

            # Run the code
            if run:
                exec(query3_python_code)
            else:
                return query3_sql_code,query3_python_code, query3_analysis


        if query.lower() == "summarize the q1’24 sales growth by regions for the drug trodelvy?":
            print("inside the answering the query")
            query4_sql_code = f'''WITH q4_2023 AS (
    SELECT 
        regn_nm,
        SUM(qty_sold_pu) AS q4_23_sales
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
    WHERE drug_name ILIKE '%trodelvy%'
        AND week_end_date BETWEEN '2023-10-01' AND '2023-12-31'
    GROUP BY regn_nm
),
q1_2024 AS (
    SELECT 
        regn_nm,
        SUM(qty_sold_pu) AS q1_24_sales
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
    WHERE drug_name ILIKE '%trodelvy%'
        AND week_end_date BETWEEN '2024-01-01' AND '2024-03-31'
    GROUP BY regn_nm
)
SELECT 
    COALESCE(q4.regn_nm, q1.regn_nm) AS regn_nm,
    COALESCE(q4.q4_23_sales, 0) AS q4_23_sales,
    COALESCE(q1.q1_24_sales, 0) AS q1_24_sales,
    COALESCE(q1.q1_24_sales, 0) - COALESCE(q4.q4_23_sales, 0) AS sales_growth,
    CASE 
        WHEN COALESCE(q4.q4_23_sales, 0) = 0 THEN NULL
        ELSE (COALESCE(q1.q1_24_sales, 0) - COALESCE(q4.q4_23_sales, 0)) / COALESCE(q4.q4_23_sales, 0) * 100
    END AS growth_percentage
FROM q4_2023 q4
FULL OUTER JOIN q1_2024 q1 ON q4.regn_nm = q1.regn_nm
ORDER BY growth_percentage DESC;'''
            table_filename = 'trodelvy_q1_24_sales_growth_by_region.png'
            query4_python_code = f'''import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Ensure the result is a DataFrame
result = pd.DataFrame(result_data)

# Data validation and cleaning
result = result.copy()
for col in ['q1_24_sales', 'q4_23_sales', 'sales_growth', 'growth_percentage']:
    result[col] = pd.to_numeric(result[col], errors='coerce')

# Sort the data by growth percentage in descending order
result = result.sort_values('growth_percentage', ascending=False)

# Create a comparison bar chart
plt.figure(figsize=(12, 6))
x = range(len(result['regn_nm']))
width = 0.35

plt.bar(x, result['q4_23_sales'], width, label='Q4 2023', color='skyblue')
plt.bar([i + width for i in x], result['q1_24_sales'], width, label='Q1 2024', color='orange')

plt.xlabel('Region')
plt.ylabel('Sales')
plt.title('Trodelvy Sales Comparison: Q4 2023 vs Q1 2024')
plt.xticks([i + width/2 for i in x], result['regn_nm'], rotation=45, ha='right')
plt.legend()

plt.tight_layout()
plt.savefig(f'Structured_Bot/output_files/{chart_name}', dpi=300, bbox_inches='tight')

# Create a styled table
plt.figure(figsize=(12, 4))
ax = plt.subplot(111, frame_on=False)
ax.xaxis.set_visible(False)
ax.yaxis.set_visible(False)

table = ax.table(cellText=result.values,
                 colLabels=result.columns,
                 cellLoc='center',
                 loc='center')

table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1.2, 1.2)

# Save table with a different filename
table_filename = chart_name.replace('.png', '_table.png')
plt.savefig(f'Structured_Bot/output_files/{table_filename}', dpi=300, bbox_inches='tight')

# Save the sales data to a CSV file
result.to_csv(f'Structured_Bot/output_files/{csv_filename}', index=False)

print("Analysis completed")
'''
                
            query4_analysis = f'''
            To summarize the Q1'24 sales growth by regions for the drug Trodelvy, we can break down the task into the following sub-tasks:
            1.**Data Source:** We will use the 'Trodelvy Sales Data' Dask dataframe.
            2.**Filter Data:** Select data of 'Trodelvy' for the first quarter (Q1) of the year 2024.
            3.**Aggregate Data:** Sum the 'qty_sold_pu' for each region ('regn_nm').
            4.**Visualization:** Plot the aggregated data to visualize the sales growth by region.
            5.**Output:** Save the aggregated data into a CSV file and save the plot as an image.
            Let's start by performing the sub-tasks. 
            '''
            if run:
                exec(query4_python_code, {"width": 0.35, "result_data": result_data, "chart_name": chart_name, "csv_filename": csv_filename})
            else:
                return query4_sql_code,query4_python_code, query4_analysis

        if query.lower() == 'calculate the top 10 parent decliners based on the sales growth percentage in recent 13 weeks for academic account type for the drug trodelvy':
            
            query6_sql_code =f"""WITH filtered_data AS (
    SELECT 
        week_end_date,
        parent_name,
        qty_sold_pu,
        top_parent_account_type,
        EXTRACT(YEAR FROM week_end_date) AS year,
        EXTRACT(MONTH FROM week_end_date) AS month
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
    WHERE LOWER(top_parent_account_type) = 'academic'
),

latest_date_cte AS (
    SELECT MAX(week_end_date) AS latest_date
    FROM filtered_data
),

-- Define the recent 13-week period (most recent 13 weeks)
recent_13w_data AS (
    SELECT 
        parent_name,
        SUM(qty_sold_pu) AS recent_13w_sales
    FROM filtered_data
    WHERE week_end_date >= (SELECT latest_date FROM latest_date_cte) - INTERVAL 12 WEEK
      AND week_end_date <= (SELECT latest_date FROM latest_date_cte)
    GROUP BY parent_name
),

-- Define the previous 13-week period (13 weeks before the recent period)
previous_13w_data AS (
    SELECT 
        parent_name,
        SUM(qty_sold_pu) AS previous_13w_sales
    FROM filtered_data
    WHERE week_end_date >= (SELECT latest_date FROM latest_date_cte) - INTERVAL 25 WEEK
      AND week_end_date <= (SELECT latest_date FROM latest_date_cte) - INTERVAL 13 WEEK
    GROUP BY parent_name
),

-- Calculate growth percentage for parents present in both periods
growth_calculation AS (
    SELECT 
        r.parent_name,
        r.recent_13w_sales,
        p.previous_13w_sales,
        CASE 
            WHEN p.previous_13w_sales > 0 THEN 
                ROUND(((r.recent_13w_sales - p.previous_13w_sales) / p.previous_13w_sales) * 100, 2)
            ELSE NULL
        END AS growth_percentage
    FROM recent_13w_data r
    INNER JOIN previous_13w_data p ON r.parent_name = p.parent_name
    WHERE p.previous_13w_sales > 0  -- Ensure we have a valid baseline
)

-- Select top 10 decliners (most negative growth)
SELECT 
    parent_name,
    recent_13w_sales,
    previous_13w_sales,
    growth_percentage,
    ABS(growth_percentage) AS decline_percentage
FROM growth_calculation
WHERE growth_percentage < 0  -- Only include decliners
ORDER BY growth_percentage ASC  -- Most negative growth first
LIMIT 10;
"""         
            query6_python_code=f'''import pandas as pd
import matplotlib.pyplot as plt

# Assuming result_data is your query result DataFrame with columns: 
# parent_name, recent_13w_sales, previous_13w_sales, growth_percentage, decline_percentage

# Create horizontal bar chart showing decline percentages
plt.figure(figsize=(12, 8))
bars = plt.barh(result_data['parent_name'], result_data['decline_percentage'], 
                color='red', alpha=0.7)

# Add percentage labels on bars using the actual decline_percentage values
for i, (bar, decline_pct) in enumerate(zip(bars, result_data['decline_percentage'])):
    width = bar.get_width()
    plt.text(width + 0.5, bar.get_y() + bar.get_height()/2, 
             decline_pct, ha='left', va='center', fontsize=10)

plt.title('Top 10 Academic Account Decliners - Trodelvy Sales', fontsize=14, fontweight='bold')
plt.xlabel('Decline Percentage (%)', fontsize=12)
plt.ylabel('Parent Account Name', fontsize=12)
plt.grid(axis='x', alpha=0.3)

plt.tight_layout()
plt.savefig(f'Structured_Bot/output_files/{chart_name}', dpi=300, bbox_inches='tight')
result_data.to_csv(f'Structured_Bot/output_files/{csv_filename}', index=False)

plt.show()'''
            query6_analysis = f'''
            To answer the query about the top 10 parent decliners based on the sales growth percentage in Recent 13 Weeks for Academic account type for the drug Trodelvy, we can break down the task into the following sub-tasks:
            1.**Data Source:** We will use the 'Trodelvy Sales Data' Dask dataframe.
            2.**Identify the Latest Date:** Determine the latest 'week_end_date' in the dataset to calculate the Recent 13 Weeks (R13W) period.
            3.**Calculate Date Range for R13W:** Use the latest date to get the date range for the Recent 13 Weeks. 
            4.**Filter Data:** Filter the data for 'Academic' account type.
            5.**Calculate Sales for R13W:** Aggregate sales data for the R13W period.
            6.**Calculate Sales for Previous 13 Weeks (P13W):** Determine the sales for the previous 13 weeks before R13W.
            7.**Calculate Sales Growth Percentage:** Compute the sales growth percentage between R13W and P13W.
            8.**Identify Top 10 Decliners:** Sort the data by sales growth percentage in ascending order to get the top decliners and select the top 10.
            Let's start by performing the sub-tasks.
            '''
            if run:
                exec(query6_python_code,{"width": 0.35, "result_data": result_data, "chart_name": chart_name, "csv_filename": csv_filename})
            else:
                return query6_sql_code,query6_python_code, query6_analysis
            

        if query.lower() == 'find weekly sales trends for texas oncology in 2024 for the drug trodelvy?':
            query7_sql_code = f'''WITH filtered_data AS (
SELECT 
    week_end_date,
    parent_name,
    qty_sold_pu,
    EXTRACT(YEAR FROM week_end_date) AS year
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
WHERE EXTRACT(YEAR FROM week_end_date) = 2024
AND LOWER(parent_name) LIKE '%texas oncology%'
)
SELECT 
week_end_date,
SUM(qty_sold_pu) AS total_qty_sold
FROM filtered_data
GROUP BY week_end_date
ORDER BY week_end_date;
'''
            query7_python_code=f'''# Create the plot for the weekly sales trend
plt.figure(figsize=(10, 5))
plt.plot(result_data['week_end_date'], 
        result_data['total_qty_sold'], 
        marker='o', linestyle='-', color='b')

# Set plot labels and title
plt.title('Weekly Sales Trends for Trodelvy at Texas Oncology in 2024')
plt.xlabel('Week End Date')
plt.ylabel('Quantity Sold')

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Add grid for better readability
plt.grid(True)

# Display the plot
plt.show()

# Save the data to a CSV file
result_data.to_csv('Structured_Bot/output_files/{csv_filename}', index=False)

# Save the plot as a PNG file
plt.savefig('Structured_Bot/output_files/{chart_name}', dpi=300, bbox_inches='tight')

            '''
            query7_analysis = f'''
            To answer the query about weekly sales trends for Texas Oncology in 2024 for the drug Trodelvy, we can break down the task into the following sub-tasks:
            1.**Data Source:** We will use the 'Trodelvy Sales ' data table.
            2.**Filter Data:** Select records for the year 2024, for the account 'Texas Oncology'.
            3.**Aggregate Data:** Group the data by 'week_end_date' and sum up the 'qty_sold_pu' to get weekly sales.
            4.**Visualization:** Plot the weekly sales trends as a line chart to visualize the data over time.
            5.**Output:** Save the aggregated data and the chart for review.
            Let's start by performing the sub-tasks. 
            '''
            if run:
                exec(query7_python_code)
            else:
                return query7_sql_code,query7_python_code, query7_analysis


        if query.lower() == 'how are sales of trodelvy vs enhertu trending in latest r13w?':
            query8_sql_code = f'''WITH latest_ddd_date AS (
    SELECT MAX(week_end_date) AS max_date
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`ddd_data_for_genie_filtered_dates`
),

date_range AS (
    SELECT 
        date_sub(max_date, ((dayofweek(max_date) + 1) % 7)) AS recent_friday,
        date_sub(date_sub(max_date, ((dayofweek(max_date) + 1) % 7)), 12 * 7) AS start_r13w
    FROM latest_ddd_date
),

tro_filtered AS (
    SELECT 
        week_end_date,
        'Trodelvy' AS drug_name,
        qty_sold_pu
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`, date_range
    WHERE week_end_date BETWEEN date_range.start_r13w AND date_range.recent_friday
),

ddd_filtered AS (
    SELECT 
        week_end_date,
        drug_name,
        trx_cnt
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`ddd_data_for_genie_filtered_dates`, date_range
    WHERE lower(drug_name) = 'enhertu'
      AND week_end_date BETWEEN date_range.start_r13w AND date_range.recent_friday
)

SELECT 
    week_end_date,
    drug_name,
    SUM(qty_sold_pu) AS qty_sold_pu
FROM (
    SELECT * FROM tro_filtered
    UNION ALL
    SELECT * FROM ddd_filtered
) AS combined_data
GROUP BY week_end_date, drug_name
ORDER BY week_end_date;

            '''
            query8_python_code=f'''# Create the plot for the sales trends
plt.figure(figsize=(10, 6))

# Plot trends for each drug
for drug in ['Trodelvy', 'Enhertu']:
    # Filter data for the current drug
    drug_data = result_data[result_data['drug_name'] == drug]
    
    # Plot the sales trend for the drug
    plt.plot(pd.to_datetime(drug_data['week_end_date']), 
            drug_data['qty_sold_pu'], label=drug)

# Add title and labels
plt.title('Weekly Sales Trends for Trodelvy and Enhertu (R13W)')
plt.xlabel('Week Ending Date')
plt.ylabel('Quantity Sold')

# Add legend and grid
plt.legend()
plt.grid(True)

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Adjust layout
plt.tight_layout()

# Save the data to a CSV file
result_data.to_csv('Structured_Bot/output_files/{csv_filename}', index=False)

# Save the plot as a PNG file
plt.savefig('Structured_Bot/output_files/{chart_name}', dpi=300, bbox_inches='tight')

# Display the plot
plt.show()
                '''


            query8_analysis = f'''
            To answer the question about how sales of Trodelvy vs Enhertu are trending in the latest R13W, we can break down the task into the following sub-tasks:
            1.**Data Source:** We will use the sales data from the 'Trodelvy Sales Data' and 'DDD Sales Data' Dask dataframe.
            2.**Identify Latest Date:** Determine the latest 'week_end_date' in the dataset to establish the most recent 13-week period.
            3.**Calculate Date Range for R13W:** Use the latest date to calculate the date range for the recent 13 weeks (R13W).
            4.**Filter Data for Trodelvy and Enhertu:** Filter the data for the drugs 'Trodelvy' and 'Enhertu'.
            5.**Aggregate Sales Data:** Aggregate the sales data by 'week_end_date' for both drugs.
            6.**Plot the Trend:** Plot the sales trend for both drugs over the R13W period.
            '''

            if run:
                exec(query8_python_code)
            else:
                return query8_sql_code,query8_python_code, query8_analysis


            
        if query.lower() == 'find weekly trodelvy sales for recent 13 weeks for high parent bc segment':
            query9_sql_code=f'''WITH latest_date AS (
    SELECT MAX(week_end_date) AS max_date
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
),
date_range AS (
    SELECT 
        max_date AS end_date,
        DATE_SUB(max_date, 84) AS start_date
    FROM latest_date
)
SELECT 
    week_end_date,
    SUM(qty_sold_pu) AS total_sales
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
WHERE week_end_date BETWEEN (SELECT start_date FROM date_range) AND (SELECT end_date FROM date_range)
    AND parent_bc_segment ILIKE 'High'
GROUP BY week_end_date
ORDER BY week_end_date
'''
            query9_python_code=f'''result_data['week_end_date'] = pd.to_datetime(result_data['week_end_date'])
# Sort by date just in case
result_data = result_data.sort_values(by='week_end_date')

# Plot
plt.figure(figsize=(10, 6))
plt.plot(result_data['week_end_date'], result_data['total_sales'], marker='o', linestyle='-')
plt.title('Weekly Trodelvy Sales for High Parent BC Segment (R13W)')
plt.xlabel('Week Ending Date')
plt.ylabel('Quantity Sold')
plt.xticks(rotation=45)
plt.grid(True)

# Save outputs
plt.savefig('Structured_Bot/output_files/{chart_name}', dpi=300, bbox_inches='tight')
result_data.to_csv('Structured_Bot/output_files/{csv_filename}', index=False)'''

            query9_analysis = f'''
            To answer the query about weekly Trodelvy sales for the recent 13 weeks for the high parent BC segment, we can break down the task into the following sub-tasks:
            1.**Data Source:** We will use the 'Trodelvy Sales Data' Dask dataframe.
            2.**Identify the Latest Date:** Determine the most recent 'week_end_date' from the sales data.
            3.**Calculate Date Range for Recent 13 Weeks (R13W):**Using the latest date, calculate the date range for the recent 13 weeks.
            4.**Filter Data for Trodelvy and High Parent BC Segment:** Filter the sales data for 'Trodelvy' drug and 'High' parent BC segment within the R13W date range.
            5.**Calculate Total Sales:** Sum up the 'qty_sold_pu' for the filtered data to get the total sales of Trodelvy for the High Parent BC segment in the recent 13 weeks.
            Let's start by performing the sub-tasks.'''
            if run:
                exec(query9_python_code)
            else:
                return query9_sql_code,query9_python_code, query9_analysis


#================================================PBC=====================================================

        if query.lower() == 'what proportion of livdelzi sales come from top 10 accounts?':
            top_10_accounts_proportion_sql_code = f'''WITH account_sales AS (
  SELECT 
    CASE 
      WHEN ship_to_snr_prnt_nm ILIKE '%NO PARENT%' THEN ship_to_poc_nm 
      ELSE ship_to_snr_prnt_nm 
    END AS account_name,
    SUM(btl_sold_eu) AS total_sales
  FROM `commercial-us-pbc-sbx-dev`.`c3po_basic`.`sd_table_for_genie`
  WHERE src_brd_nm ILIKE '%LIVDELZI%'
  GROUP BY 1
),
total_sales AS (
  SELECT SUM(total_sales) AS grand_total
  FROM account_sales
),
top_10_accounts AS (
  SELECT 
    account_name,
    total_sales,
    ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS rank
  FROM account_sales
  ORDER BY total_sales DESC
  LIMIT 10
)
SELECT 
  t.account_name,
  t.total_sales,
  t.rank,
  t.total_sales / ts.grand_total AS proportion_of_total,
  SUM(t.total_sales) OVER () / ts.grand_total AS top_10_proportion
FROM top_10_accounts t
CROSS JOIN total_sales ts
ORDER BY t.rank'''
            
            top_10_accounts_proportion_python_code=f'''import pandas as pd
import matplotlib.pyplot as plt
import os
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)
# Assuming the result from SQL query is stored in 'result_data' DataFrame
result_data['proportion_of_total'] = result_data['proportion_of_total'] * 100 
top_10_proportion = result_data['top_10_proportion'].iloc[0] * 100


# Create a summary DataFrame
summary = pd.DataFrame({{
    'Category': ['Top 10 Accounts', 'Other Accounts'],
    'Proportion': [top_10_proportion, 100 - top_10_proportion]
}})

# Create a pie chart for summary
plt.figure(figsize=(10, 6))
plt.pie(summary['Proportion'], labels=summary['Category'], autopct='%1.1f%%', startangle=90)
plt.title('Proportion of Livdelzi Sales: Top 10 Accounts vs Others')
plt.axis('equal')


# Define file paths
csv_output_path = os.path.join(output_dir, csv_filename)
img_output_path = os.path.join(output_dir, chart_name)

# Save output files
plt.savefig(img_output_path, dpi=300, bbox_inches='tight')
result_data.to_csv(csv_output_path, index=False)

# Confirmation
print(f"Saved CSV: csv_output_path")
print(f"Saved Image: img_output_path ")

            '''
            top_10_accounts_proportion_analysis = f'''To answer this question about the proportion of Livdelzi sales coming from the top 10 accounts, we need to break down the query into sub-tasks and use the appropriate SQL code to retrieve the data. Then, we'll use Python to process the results and create visualizations. Let's go through this step-by-step:\n
\n
a. Explain the approach briefly:
    We'll first query the SD table to get the total Livdelzi sales and the sales by account. Then, we'll identify the top 10 accounts by sales volume and calculate the proportion of total sales they represent.
b. Split the user question into clear sub-tasks:
    Get total Livdelzi sales
    Get Livdelzi sales by account
    Identify top 10 accounts by sales volume
    Calculate the proportion of sales from top 10 accounts  
c. Process the data and create visualizations using Python.
Let's start with the first sub-task of filtering the data.'''

            # Run the code
            if run:
                exec(top_10_accounts_proportion_python_code)
            else:
                return top_10_accounts_proportion_sql_code,top_10_accounts_proportion_python_code,top_10_accounts_proportion_analysis
        if query.lower() in ['how many bottles were shipped since launch?']:
            bottles_since_launch_sql_code = f'''WITH launch_date AS (
  SELECT DATE('2024-08-14') AS launch_date
)
SELECT 
  bus_wk_end_dt AS week_end_date,
  SUM(btl_sold_eu) AS bottles_shipped
FROM `commercial-us-pbc-sbx-dev`.`c3po_basic`.`sd_table_for_genie`
WHERE src_brd_nm ILIKE '%LIVDELZI%'
  AND bus_wk_end_dt >= (SELECT launch_date FROM launch_date)
GROUP BY bus_wk_end_dt
ORDER BY bus_wk_end_dt'''
            bottles_since_launch_python_code=f'''import pandas as pd
import matplotlib.pyplot as plt
import os
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)
# Assuming the SQL query result is stored in the 'result_data' DataFrame
result_data['week_end_date'] = pd.to_datetime(result_data['week_end_date'])
result_data = result_data.sort_values('week_end_date')

# Calculate the total bottles shipped
total_bottles_shipped = result_data['bottles_shipped'].sum()

# Create a line plot of bottles shipped over time
plt.figure(figsize=(12, 6))
plt.plot(result_data['week_end_date'], result_data['bottles_shipped'], marker='o')
plt.title(f'Livdelzi Bottles Shipped Since Launch (Total: {{total_bottles_shipped:.0f}})')
plt.xlabel('Week End Date')
plt.ylabel('Bottles Shipped')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

csv_output_path = os.path.join(output_dir, csv_filename)
img_output_path = os.path.join(output_dir, chart_name)

plt.savefig(img_output_path, dpi=300, bbox_inches='tight')
# Save results to CSV
result_data.to_csv(csv_output_path)

# Step 7: Confirmation
print(f"Saved CSV: csv_output_path")
print(f"Saved Image: img_output_path")

            '''
            bottles_since_launch_analysis = f'''To answer this question about the number of bottles shipped since the launch of Livdelzi, we need to follow a structured approach. Let's break down the query and use the appropriate tools to retrieve and analyze the data.
a. Approach:
We'll use SQL to fetch the data from the SD table, filtering for Livdelzi shipments since the launch date. Then, we'll use Python to process the results and create a visualization.
b. Sub-tasks:
Retrieve the total number of bottles shipped for Livdelzi since the launch date.
Visualize the shipment data over time.'''
            if run:
                exec(bottles_since_launch_python_code)
            else:
                return bottles_since_launch_sql_code,bottles_since_launch_python_code, bottles_since_launch_analysis
        if query.lower() in ['how many accounts sold livdelzi since launch?']:

            accounts_sold_liv_sql_code = f'''WITH account_sales AS (
  SELECT
    CASE 
      WHEN ship_to_snr_prnt_nm ILIKE '%NO PARENT%' THEN ship_to_poc_nm
      ELSE ship_to_snr_prnt_nm
    END AS account_name,
    SUM(btl_sold_eu) AS total_sales
  FROM `commercial-us-pbc-sbx-dev`.`c3po_basic`.`sd_table_for_genie`
  WHERE src_brd_nm ILIKE '%Livdelzi%'
    AND bus_wk_end_dt >= DATE '2024-08-14'  -- Launch date of Livdelzi
  GROUP BY 1
  HAVING SUM(btl_sold_eu) > 0
)
SELECT COUNT(DISTINCT account_name) AS accounts_sold_livdelzi
FROM account_sales;'''
            accounts_sold_liv_python_code=f'''import matplotlib.pyplot as plt
import pandas as pd
import os
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)
# Assuming the result is already in a pandas DataFrame called 'result_data'

# Create a bar chart
plt.figure(figsize=(10, 6))
plt.bar(['Accounts Sold Livdelzi'], result_data['accounts_sold_livdelzi'])
plt.title('Number of Accounts that Sold Livdelzi Since Launch')
plt.ylabel('Number of Accounts')
plt.ylim(0, result_data['accounts_sold_livdelzi'].values[0] * 1.2)  # Set y-axis limit to 120% of the value

# Add value label on top of the bar
plt.text(0, result_data['accounts_sold_livdelzi'].values[0], str(result_data['accounts_sold_livdelzi'].values[0]), 
         ha='center', va='bottom')
# Save image
csv_output_path = os.path.join(output_dir, csv_filename)
output_img_path = os.path.join(output_dir, chart_name)

plt.savefig(output_img_path, bbox_inches='tight')
result_data.to_csv(csv_output_path)

# Optional confirmation
print(f"Saved CSV: output_csv_path")
print(f"Saved Image: output_img_path")'''
            accounts_sold_liv_analysis = f'''To answer this question, we need to analyze the data for Livdelzi sales since its launch date. Let's break down the approach and use the appropriate tools to retrieve and process the data.
a. Approach:
Determine the launch date of Livdelzi (August 14, 2024)
Query the SD table for Livdelzi sales since the launch date
Count the distinct accounts that have sold Livdelzi
Visualize the results
b. Sub-tasks:
Write SQL query to get the count of distinct accounts selling Livdelzi since launch
Generate a visualization of the results'''
            if run:
                print("Hello C3PO while executing")
                exec(accounts_sold_liv_python_code)
            else:
                print("Hello C3PO before return")
                return accounts_sold_liv_sql_code, accounts_sold_liv_python_code,accounts_sold_liv_analysis

        if query.lower() in ['what percentage of  bottles are sold by each account?']:

            bottle_sold_by_account_sql_code = f'''WITH account_sales AS (
  SELECT
    CASE
      WHEN ship_to_snr_prnt_nm ILIKE '%NO PARENT%' THEN ship_to_poc_nm
      ELSE ship_to_snr_prnt_nm
    END AS account_name,
    SUM(btl_sold_eu) AS total_bottles_sold
  FROM `commercial-us-pbc-sbx-dev`.`c3po_basic`.`sd_table_for_genie`
  WHERE ship_to_snr_prnt_nm IS NOT NULL OR ship_to_poc_nm IS NOT NULL
  GROUP BY 1
)
SELECT
  account_name,
  total_bottles_sold,
  (total_bottles_sold * 100.0 / SUM(total_bottles_sold) OVER ()) AS percentage_sold
FROM account_sales
ORDER BY total_bottles_sold DESC'''
            bottle_sold_by_account_python_code=f'''import pandas as pd
import matplotlib.pyplot as plt
import os
output_dir = "Structured_Bot/output_files"
# Assuming the SQL query result is stored in the 'result_data' DataFrame
# Sort the DataFrame by percentage_sold in descending order
result_sorted = result_data.sort_values('percentage_sold', ascending=False)

# Select top 10 accounts for visualization
top_10_accounts = result_sorted.head(10)

# Create a bar chart
plt.figure(figsize=(12, 6))
plt.bar(top_10_accounts['account_name'], top_10_accounts['percentage_sold'])
plt.title('Top 10 Accounts by Percentage of Bottles Sold')
plt.xlabel('Account Name')
plt.ylabel('Percentage of Bottles Sold')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()

# Save image
csv_output_path = os.path.join(output_dir, csv_filename)
output_img_path = os.path.join(output_dir, chart_name)

plt.savefig(output_img_path, bbox_inches='tight')
result_data.to_csv(csv_output_path)

# Optional confirmation
print(f"Saved CSV: output_csv_path")
print(f"Saved Image: output_img_path")'''
            bottle_sold_by_account_analysis = f'''To answer this question about the percentage of bottles sold by each account, I'll need to generate SQL code to retrieve the data and then use Python for further processing and visualization. Let's break down the approach:
a. Explain the approach briefly:
We'll first query the SD data to get the total bottles sold for each account, then calculate the percentage for each account.
b. Split the user question into clear sub-tasks:
Retrieve total bottles sold for each account from the SD table
Calculate the percentage of bottles sold by each account
Visualize the results'''
            if run:
                print("Hello C3PO while executing")
                exec(bottle_sold_by_account_python_code)
            else:
                print("Hello C3PO before return")
                return bottle_sold_by_account_sql_code, bottle_sold_by_account_python_code, bottle_sold_by_account_analysis
            