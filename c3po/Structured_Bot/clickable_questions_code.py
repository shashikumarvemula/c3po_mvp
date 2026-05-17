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


class run_clickable_questions:
    def __init__(self):
        pass

    def run_clickable(self, result_data, query, run, chart_name , csv_filename): # whole_data
        print("in refresh file!!!!!!!!!!")
        print("Hello C3PO - 1")
        if query.lower() == 'what is the nps share by regimen group in lot 2 and tier 1 hcps in tnbc?':
            tier1_nps_sql_code = f'''SELECT
    regimen_grouped_name AS Regimen,
    SUM(Monthly_Adjusted_Count) AS Total_Monthly_Adjusted_Count
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`
WHERE
    TRIM(Line_of_Therapy_LOT) = '2'
    AND LOWER(TRIM(NPI_HCP_tier)) = 'tier 1'
    AND LOWER(TRIM(indication)) = LOWER('{"tnbc"}')
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
plt.bar(grouped_data_share_df['Regimen'], grouped_data_share_df['Share'], color='skyblue')
plt.title(f'NPS Share by Regimen Group in LOT 2 and Tier 1 HCPs in {"tnbc"}')
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
print(f"Saved CSV: csv_output_path")
print(f"Saved Image: img_output_path ")

            '''
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

            sob_sql_code = f'''SELECT
    source_of_business,
    COUNT(*) AS nps_count
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`
WHERE
    LOWER(regimen_grouped_name) LIKE '%trodelvy%'
    AND Line_of_Therapy_LOT = '2'
    AND Quarter = '2'
    AND Year = '2024'
    AND LOWER(TRIM(indication)) = LOWER('{"tnbc"}')
    AND new_patient_flag = '1'
GROUP BY
    source_of_business'''
            sob_python_code=f'''import os 
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)
grouped_data_computed = result_data.set_index('source_of_business')['nps_count']
total_count = grouped_data_computed.sum()
shares = grouped_data_computed / total_count

# Save CSV
output_csv_path = os.path.join(output_dir, csv_filename)
shares.to_csv(output_csv_path, index=True)

# Plot Pie Chart
plt.figure(figsize=(12, 10))
labels = shares.index
wedges, texts, autotexts = plt.pie(
    shares,
    autopct='%1.1f%%',
    startangle=90,
    colors=plt.cm.Paired(range(len(shares)))
)
plt.legend(wedges, labels, title="Categories", loc="best")
plt.axis('equal')  # Ensure pie is a circle
plt.title(f"Source of Business Share for Trodelvy in LOT 2 during Q2 2024 in {"tnbc"}")
plt.ylabel('')

# Save image
output_img_path = os.path.join(output_dir, chart_name)
plt.savefig(output_img_path, bbox_inches='tight')

# Optional confirmation
print(f"Saved CSV: output_csv_path")
print(f"Saved Image: output_img_path")'''
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
plt.bar(grouped_data_share_df['Regimen'], grouped_data_share_df['Share'], color='skyblue')
plt.title(f'NPS Share by Regimen Group in LOT 2 and Tier 1 HCPs in {"hr+/her2-"}')
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
            sob_python_code=f'''import os 
output_dir = "Structured_Bot/output_files"
os.makedirs(output_dir, exist_ok=True)
grouped_data_computed = result_data.set_index('source_of_business')['nps_count']
total_count = grouped_data_computed.sum()
shares = grouped_data_computed / total_count

# Save CSV
output_csv_path = os.path.join(output_dir, csv_filename)
shares.to_csv(output_csv_path, index=True)

# Plot Pie Chart
plt.figure(figsize=(12, 10))
labels = shares.index
wedges, texts, autotexts = plt.pie(
    shares,
    autopct='%1.1f%%',
    startangle=90,
    colors=plt.cm.Paired(range(len(shares)))
)
plt.legend(wedges, labels, title="Categories", loc="best")
plt.axis('equal')  # Ensure pie is a circle
plt.title(f"Source of Business Share for Trodelvy in LOT 2 during Q2 2024 in {"hr+/her2-"}")
plt.ylabel('')

# Save image
output_img_path = os.path.join(output_dir, chart_name)
plt.savefig(output_img_path, bbox_inches='tight')

# Optional confirmation
print(f"Saved CSV: output_csv_path")
print(f"Saved Image: output_img_path")'''
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
            query1_sql_code =f"""WITH latest_date_cte AS (
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
        EXTRACT(YEAR FROM week_end_date) AS year,
        EXTRACT(MONTH FROM week_end_date) AS month,
        DATE_FORMAT(week_end_date, 'yyyy-MM') AS year_month,
        qty_sold_pu
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
    WHERE EXTRACT(YEAR FROM week_end_date) >= 2020
      AND week_end_date <= (SELECT previous_week_date FROM previous_week_cte)
)
SELECT 
    year_month,
    SUM(qty_sold_pu) AS total_qty_sold
FROM filtered_data
GROUP BY year_month
ORDER BY year_month ASC;
                """
            query1_python_code=f'''fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(result_data['year_month'], 
result_data['total_qty_sold'], 
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
            query4_sql_code = f'''WITH filtered_data AS (
SELECT 
    week_end_date,
    regn_nm,
    qty_sold_pu,
    EXTRACT(YEAR FROM week_end_date) AS year,
    EXTRACT(MONTH FROM week_end_date) AS month
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
WHERE EXTRACT(YEAR FROM week_end_date) = 2024
AND EXTRACT(MONTH FROM week_end_date) IN (1, 2, 3)
)
SELECT 
regn_nm,
SUM(qty_sold_pu) AS total_qty_sold
FROM filtered_data
GROUP BY regn_nm
ORDER BY total_qty_sold DESC;'''
            query4_python_code = f'''# Sort the sales data by region total
trodelvy_sales_by_region_sorted= result_data.sort_values(by='total_qty_sold', ascending=True)

# Create the horizontal bar plot
plt.figure(figsize=(10, 8))
plt.barh(trodelvy_sales_by_region_sorted['regn_nm'], trodelvy_sales_by_region_sorted['total_qty_sold'], color='skyblue')

# Set labels and title
plt.xlabel('Total Quantity Sold')
plt.ylabel('Region')
plt.title("Q1'24 Sales of Trodelvy by Region")

# Add grid for better readability
plt.grid(True)

# Display the plot
plt.show()

# Save the plot as a PNG file
plt.savefig(f'Structured_Bot/output_files/{chart_name}', dpi=300, bbox_inches='tight')

# Save the sales data to a CSV file
trodelvy_sales_by_region_sorted.to_csv(f'Structured_Bot/output_files/{csv_filename}', index=False)
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
                exec(query4_python_code)
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

previous_week_cte AS (
    SELECT MAX(week_end_date) AS previous_week_date
    FROM filtered_data
    WHERE week_end_date < (SELECT latest_date FROM latest_date_cte)
),

r13w_data AS (
    SELECT 
        week_end_date,
        parent_name,
        qty_sold_pu
    FROM filtered_data
    WHERE week_end_date >= (SELECT previous_week_date FROM previous_week_cte) - INTERVAL 12 WEEK
      AND week_end_date <= (SELECT previous_week_date FROM previous_week_cte)
)

SELECT 
    parent_name,
    SUM(qty_sold_pu) AS total_qty_sold
FROM r13w_data
GROUP BY parent_name
ORDER BY total_qty_sold ASC
LIMIT 10;
"""
            query6_python_code=f'''# Save the data to a CSV file
result_data.to_csv(f'Structured_Bot/output_files/{csv_filename}', index=False)

            '''
            query6_analysis = f'''
            To answer the query about the top 10 parent decliners based on the sales growth percentage in Recent 13 Weeks for Academic account type for the drug Trodelvy, we can break down the task into the following sub-tasks:\n
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
                exec(query6_python_code)
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
AND LOWER(parent_name) LIKE '%texas%oncology%'
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
            query9_sql_code=f'''WITH latest_dates AS (
    SELECT 
        MAX(week_end_date) AS latest_date
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
),
previous_week AS (
    SELECT 
        MAX(week_end_date) AS previous_week_date
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
    WHERE week_end_date < (SELECT latest_date FROM latest_dates)
),
recent_13_weeks AS (
    SELECT 
        -- Move back to the most recent Friday from previous_week_date
        date_sub(previous_week_date, (dayofweek(previous_week_date) + 1) % 7) AS most_recent_friday
    FROM previous_week
),
r13w_range AS (
    SELECT 
        date_sub(most_recent_friday, 7 * 12) AS start_r13w,
        most_recent_friday AS end_r13w
    FROM recent_13_weeks
)
SELECT 
    week_end_date,
    SUM(qty_sold_pu) AS total_qty_sold
FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`
WHERE LOWER(parent_bc_segment) = 'high'
  AND week_end_date BETWEEN (SELECT start_r13w FROM r13w_range)
                         AND (SELECT end_r13w FROM r13w_range)
GROUP BY week_end_date
ORDER BY week_end_date;'''
            query9_python_code=f'''result_data['week_end_date'] = pd.to_datetime(result_data['week_end_date'])
# Sort by date just in case
result_data = result_data.sort_values(by='week_end_date')

# Plot
plt.figure(figsize=(10, 6))
plt.plot(result_data['week_end_date'], result_data['total_qty_sold'], marker='o', linestyle='-')
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