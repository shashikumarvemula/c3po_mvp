import pandas as pd
def get_date_instruction(warehouse):
    sql_query_867="""SELECT MAX(week_end_date) AS max_date
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`867_data_drug-trodelvy_for_genie_filtered_dates`;
    """
    sql_query_ddd="""SELECT MAX(week_end_date) AS max_date
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`ddd_data_for_genie_filtered_dates`;
    """
    sql_query_claims="""SELECT MAX(Year_Month) AS max_date
    FROM `commercial-us-apo-onc-iia-dev`.`setuserv_adhocs`.`onc_claims_data`;
    """
    # sample=pd.DataFrame(cursor.execute(sql_code).fetchall(),columns=[elem[0] for elem in cursor.description])
    sample=warehouse.get_data_from_wareshouse(sql_query_867)
    latest_date_867=str(sample['max_date'].iloc[0])
    sample=warehouse.get_data_from_wareshouse(sql_query_ddd)
    latest_date_ddd=str(sample['max_date'].iloc[0])
    sample=warehouse.get_data_from_wareshouse(sql_query_claims)
    latest_date_claims=str(sample['max_date'].iloc[0])
    instructions=f"""
    The latest date for DataSource_Sales_867 is {latest_date_867}.
    The latest date for DataSource_Sales_DDD is {latest_date_ddd}.
    The latest date for DataSource_Claims is {latest_date_claims}.
    """
    return instructions