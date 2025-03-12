from google.cloud import bigquery

def create_italy_query(bq_ops, temp_table_id):
    """Create BigQuery SQL for Italy data with corrected Total Group retention metrics."""
    # Get Italy-specific filters from country filters section
    country_code = "ITA"
    italy_filters = bq_ops.config.get(f"COUNTRIES_FILTERS_{country_code}", {})
    excluded_chains = italy_filters.get("excluded_chain_types", [])
    
    # Create parameter placeholders for excluded chains
    excluded_chain_params = []
    for i, chain in enumerate(excluded_chains):
        excluded_chain_params.append(f"@excluded_chain_{i}")
    
    excluded_chains_sql = ', '.join(excluded_chain_params)
    
    # Get country mappings from config
    country_mapping = bq_ops.config.get("COUNTRIES_MAPPING", {"ITA": "IT", "ESP": "SP"})
    
    # Generate dynamic CASE statement for country mapping
    country_mapping_cases = []
    for source, target in country_mapping.items():
        country_mapping_cases.append(f"WHEN line.COUNTRY_KEY = '{source}' THEN '{target}'")
    
    country_mapping_sql = "\n                ".join(country_mapping_cases)
    if not country_mapping_sql:
        country_mapping_sql = "ELSE line.COUNTRY_KEY"  # Default fallback
    
    # Rest of the query template remains the same...
    query = f"""
    CREATE OR REPLACE TABLE {temp_table_id} AS
    -- Main data aggregation CTE
    WITH data_agg AS (
        SELECT
            DATE_TRUNC(line.DATE_KEY, MONTH) AS month,
            CASE 
                {country_mapping_sql}
                ELSE line.COUNTRY_KEY 
            END AS country_key,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce'
                ELSE store.CHAIN_TYPE_DESC
            END AS chain_type_desc,

            -- Pre-aggregate metrics with conditional filtering in one pass
            SUM(line.SAL_AMT_WTAX * line.EURO_RATE) AS CA_Tous_Clients,
            SUM(CASE WHEN line.COUNTRY_KEY IS NULL THEN line.SAL_AMT_WTAX ELSE NULL END) AS CA_Tous_Clients_local,
            SUM(CASE WHEN line.CARD_FLAG = '1' THEN line.SAL_AMT_WTAX * line.EURO_RATE END) AS CA_Porteurs,
            SUM(CASE WHEN line.CARD_FLAG = '0' AND line.COUNTRY_KEY IS NULL THEN line.SAL_AMT_WTAX END) AS CA_Porteurs_local,
            SUM(CASE WHEN line.PROMO_FLAG = '1' THEN line.SAL_AMT_WTAX * line.EURO_RATE END) AS CA_promo,
            SUM(CASE WHEN line.PROMO_FLAG = '0' AND line.COUNTRY_KEY IS NULL THEN line.SAL_AMT_WTAX END) AS CA_promo_local,
            
            -- Transaction counts with FORMAT instead of MD5 for better performance
            COUNT(DISTINCT FORMAT('%s|%s|%s|%s|%s|%s', 
                header.COUNTRY_KEY, CAST(header.DATE_KEY AS STRING), header.SITE_KEY,
                header.TRX_KEY, header.TRX_TYPE, header.REC_SRC)) AS Nb_transactions_Tous_Clients,
            
            COUNT(DISTINCT CASE WHEN line.CARD_FLAG = '1' THEN FORMAT('%s|%s|%s|%s|%s|%s', 
                header.COUNTRY_KEY, CAST(header.DATE_KEY AS STRING), header.SITE_KEY,
                header.TRX_KEY, header.TRX_TYPE, header.REC_SRC) END) AS Nb_transactions_porteurs,
            
            -- Household count
            COUNT(DISTINCT CASE WHEN card.CARD_KEY <> '-1' THEN card.ACCOUNT_KEY END) AS Nb_foyers,
            
            -- Article counts
            SUM(line.sal_unit_qty) AS nb_articles,
            SUM(CASE WHEN line.CARD_FLAG = '1' THEN line.sal_unit_qty END) AS nb_articles_porteur,
            
            -- Placeholder for additional metrics
            CAST(NULL AS INT64) AS Families,
            CAST(NULL AS INT64) AS Seniors

        FROM `{bq_ops.config.get("SOURCE_LINE_TABLE")}` AS line
        -- Use JOIN hints to optimize large joins
        LEFT JOIN `{bq_ops.config.get("SOURCE_HEADER_TABLE")}` AS header
          ON line.DATE_KEY = header.DATE_KEY
         AND line.COUNTRY_KEY = header.COUNTRY_KEY
         AND line.SITE_KEY = header.SITE_KEY
         AND line.TRX_KEY = header.TRX_KEY
         AND line.TRX_TYPE = header.TRX_TYPE
         AND line.rec_src = header.rec_src
        LEFT JOIN `{bq_ops.config.get("SOURCE_CARD_TABLE")}` AS card
          ON header.CARD_KEY = card.CARD_KEY
        -- Use INNER JOIN which is more efficient when we need all records
        INNER JOIN `{bq_ops.config.get("SOURCE_SITE_TABLE")}` AS site
          ON line.SITE_KEY = site.site_key
         AND line.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config.get("SOURCE_STORE_TABLE")}` AS store
          ON site.MAIN_SITE_KEY = store.STORE_KEY
         AND site.COUNTRY_KEY = store.COUNTRY_KEY

        WHERE DATE_TRUNC(line.date_key, MONTH) = @theMonth
          AND store.chain_type_key != "CAC"
          AND line.COUNTRY_KEY = 'ITA'
          AND store.CHAIN_TYPE_DESC NOT IN ({excluded_chains_sql})

        GROUP BY 
            GROUPING SETS (
                (month, country_key, chain_type_desc), -- Detailed level
                (month, country_key),                  -- Country level
                (month)                                -- Total level
            )
    ),
    
    -- Rest of the query remains the same...
    """
    
    # Existing parameter handling code...
    # Create parameters list for the query
    params = [bigquery.ScalarQueryParameter("theMonth", "DATE", None)]  # Main parameter for @theMonth
    
    # Add parameters for excluded chains
    for i, chain in enumerate(excluded_chains):
        params.append(bigquery.ScalarQueryParameter(f"excluded_chain_{i}", "STRING", chain))
    
    # Return both the query and the parameters
    return query, params