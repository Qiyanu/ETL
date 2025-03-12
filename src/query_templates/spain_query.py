from google.cloud import bigquery

def create_spain_query(bq_ops, temp_table_id):
    """Create BigQuery SQL for Spain data with corrected Total Group retention metrics."""
    # Get Spain-specific filters from country filters section
    country_code = "ESP"
    spain_filters = bq_ops.config.get(f"COUNTRIES_FILTERS_{country_code}", {})
    
    # Get excluded business brands
    excluded_business_brands = spain_filters.get("excluded_business_brands", ["supeco"])
    
    # Prepare parameter placeholders for excluded business brands
    excluded_business_params = []
    for i, brand in enumerate(excluded_business_brands):
        excluded_business_params.append(f"@excluded_brand_{i}")
    
    # Format excluded business brands using parameter placeholders
    if excluded_business_params:
        excluded_business_brands_sql = " OR ".join([f"lower(header.business_brand) = {param}" for param in excluded_business_params])
        excluded_business_brands_condition = f"AND NOT ({excluded_business_brands_sql})"
    else:
        excluded_business_brands_condition = ""
        
    # Format ECM exclusions using parameter placeholders
    ecm_exclusions = spain_filters.get("excluded_ecm_combinations", [])
    ecm_conditions = []
    ecm_params = []
    
    for i, exclusion in enumerate(ecm_exclusions):
        conditions = []
        for key, value in exclusion.items():
            param_name = f"@ecm_param_{i}_{key}"
            conditions.append(f"header.{key} = {param_name}")
            ecm_params.append((param_name, value))
    
        if conditions:
            ecm_conditions.append(" AND ".join(conditions))
    
    if ecm_conditions:
        ecm_condition = f"AND NOT (header.activity_type = 'ECM' AND ({' OR '.join(ecm_conditions)}))"
    else:
        ecm_condition = ""
    
    # Get country mappings from config
    country_mapping = bq_ops.config.get("COUNTRIES_MAPPING", {"ITA": "IT", "ESP": "SP"})
    
    # Generate dynamic CASE statement for country mapping
    country_mapping_cases = []
    for source, target in country_mapping.items():
        country_mapping_cases.append(f"WHEN line.COUNTRY_KEY = '{source}' THEN '{target}'")
    
    country_mapping_sql = "\n                ".join(country_mapping_cases)
    if not country_mapping_sql:
        country_mapping_sql = "ELSE line.COUNTRY_KEY"  # Default fallback

    # Rest of the query remains the same...
    query = f"""
    CREATE OR REPLACE TABLE {temp_table_id} AS
    WITH data_agg AS (
        SELECT
            DATE_TRUNC(line.DATE_KEY, MONTH) AS month,
            CASE WHEN line.COUNTRY_KEY = 'ESP' THEN 'SP' END AS COUNTRY_KEY,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce' 
                ELSE store.CHAIN_TYPE_DESC 
            END AS CHAIN_TYPE_DESC,
            
            SUM(line.SAL_AMT_WTAX * line.EURO_RATE) AS total_sales,
            SUM(CASE WHEN header.CUST_ID IS NOT NULL THEN line.SAL_AMT_WTAX * line.EURO_RATE END) AS sales_cardholders,
            
            COUNT(DISTINCT FORMAT('%s|%s|%s|%s|%s|%s', 
                header.COUNTRY_KEY, CAST(header.DATE_KEY AS STRING), header.SITE_KEY,
                header.TRX_KEY, header.TRX_TYPE, header.REC_SRC)) AS nb_trx,
            
            COUNT(DISTINCT CASE WHEN header.CUST_ID IS NOT NULL THEN FORMAT('%s|%s|%s|%s|%s|%s', 
                header.COUNTRY_KEY, CAST(header.DATE_KEY AS STRING), header.SITE_KEY,
                header.TRX_KEY, header.TRX_TYPE, header.REC_SRC) END) AS nb_trx_cardholders,
            
            COUNT(DISTINCT CASE WHEN header.CUST_ID IS NOT NULL THEN header.CUST_ID END) AS nb_cardholders,
            
            SUM(line.sal_unit_qty) AS sales_qty,
            SUM(CASE WHEN header.CUST_ID IS NOT NULL THEN line.sal_unit_qty END) AS sales_qty_carholders,
            
            CAST(NULL AS INT64) AS Families,
            CAST(NULL AS INT64) AS Seniors,
            
            SUM(CASE WHEN line.PROMO_FLAG = '1' THEN line.SAL_AMT_WTAX * line.EURO_RATE END) AS sales_promo
            
        FROM `{bq_ops.config["SOURCE_LINE_TABLE"]}` AS line
        LEFT JOIN `{bq_ops.config["SOURCE_HEADER_TABLE"]}` AS header
            ON line.DATE_KEY = header.DATE_KEY
            AND line.COUNTRY_KEY = header.COUNTRY_KEY
            AND line.SITE_KEY = header.SITE_KEY
            AND line.TRX_KEY = header.TRX_KEY
            AND line.TRX_TYPE = header.TRX_TYPE
            AND line.rec_src = header.rec_src
        INNER JOIN `{bq_ops.config["SOURCE_SITE_TABLE"]}` AS site
            ON line.SITE_KEY = site.site_key
            AND line.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config["SOURCE_STORE_TABLE"]}` AS store
            ON site.MAIN_SITE_KEY = store.STORE_KEY
            AND site.COUNTRY_KEY = store.COUNTRY_KEY
            
        WHERE DATE_TRUNC(line.date_key, MONTH) = @theMonth
            AND store.chain_type_key != "CAC"
            AND line.COUNTRY_KEY = 'ESP'
            {excluded_business_brands_condition}
            {ecm_condition}
            
        GROUP BY 
            GROUPING SETS (
                (month, COUNTRY_KEY, CHAIN_TYPE_DESC),
                (month, COUNTRY_KEY),
                (month)
            )
    ),
    
    -- Rest of the query remains the same...
    """
    
    # Create parameters list for the query
    params = [bigquery.ScalarQueryParameter("theMonth", "DATE", None)]  # Main parameter for @theMonth
    
    # Add parameters for excluded business brands
    for i, brand in enumerate(excluded_business_brands):
        params.append(bigquery.ScalarQueryParameter(f"excluded_brand_{i}", "STRING", brand.lower()))
    
    # Add parameters for ECM exclusions
    for param_name, value in ecm_params:
        # Extract parameter name without the @ symbol
        clean_param_name = param_name[1:]
        params.append(bigquery.ScalarQueryParameter(clean_param_name, "STRING", value))
    
    # Return both the query and the parameters
    return query, params