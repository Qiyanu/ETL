from google.cloud import bigquery

def create_spain_query(bq_ops, temp_table_id):
    """Create BigQuery SQL for Spain data with corrected Total Group retention metrics."""
    # Get filters for Spain
    spain_filters = bq_ops.config.get("SPAIN_FILTERS", {})
    excluded_business_brands = spain_filters.get("excluded_business_brands", ["supeco"])
    
    # Format excluded business brands
    excluded_business_brands_sql = " OR ".join([f"lower(header.business_brand) = '{brand.lower()}'" for brand in excluded_business_brands])
    if excluded_business_brands_sql:
        excluded_business_brands_condition = f"AND NOT ({excluded_business_brands_sql})"
    else:
        excluded_business_brands_condition = ""
        
    # Format ECM exclusions
    ecm_exclusions = spain_filters.get("excluded_ecm_combinations", [])
    ecm_conditions = []
    for exclusion in ecm_exclusions:
        conditions = []
        for key, value in exclusion.items():
            conditions.append(f"header.{key} = '{value}'")
        if conditions:
            ecm_conditions.append(" AND ".join(conditions))
    
    if ecm_conditions:
        ecm_condition = f"AND NOT (header.activity_type = 'ECM' AND ({' OR '.join(ecm_conditions)}))"
    else:
        ecm_condition = ""
    
    # Get country mappings from config
    country_mapping = bq_ops.config.get("COUNTRY_MAPPING", {"ITA": "IT", "ESP": "SP"})
    
    # Generate dynamic CASE statement for country mapping
    country_mapping_cases = []
    for source, target in country_mapping.items():
        country_mapping_cases.append(f"WHEN line.COUNTRY_KEY = '{source}' THEN '{target}'")
    
    country_mapping_sql = "\n                ".join(country_mapping_cases)
    if not country_mapping_sql:
        country_mapping_sql = "ELSE line.COUNTRY_KEY"  # Default fallback

    # Updated Spain query with fixed Total Group retention metrics
    return f"""
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
    
    last12_month AS (
        SELECT
            CASE WHEN header.CUST_ID IS NOT NULL THEN header.CUST_ID END AS id_client,
            CASE WHEN line.COUNTRY_KEY = 'ESP' THEN 'SP' END AS COUNTRY_KEY,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce' 
                ELSE store.CHAIN_TYPE_DESC 
            END AS CHAIN_TYPE_DESC
            
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
            
        WHERE DATE_TRUNC(line.date_key, MONTH)
              BETWEEN DATE_SUB(@theMonth, INTERVAL 11 MONTH) AND @theMonth
            AND store.chain_type_key != "CAC"
            AND line.COUNTRY_KEY = 'ESP'
            {excluded_business_brands_condition}
            {ecm_condition}
        
        GROUP BY 1, 2, 3
    ),
    
    previous12_month AS (
        SELECT
            CASE WHEN header.CUST_ID IS NOT NULL THEN header.CUST_ID END AS id_client,
            CASE WHEN line.COUNTRY_KEY = 'ESP' THEN 'SP' END AS COUNTRY_KEY,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce' 
                ELSE store.CHAIN_TYPE_DESC 
            END AS CHAIN_TYPE_DESC
            
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
            
        WHERE DATE_TRUNC(line.date_key, MONTH)
              BETWEEN DATE_SUB(@theMonth, INTERVAL 23 MONTH) 
                  AND DATE_SUB(@theMonth, INTERVAL 12 MONTH)
            AND store.chain_type_key != "CAC"
            AND line.COUNTRY_KEY = 'ESP'
            {excluded_business_brands_condition}
            {ecm_condition}
            
        GROUP BY 1, 2, 3
    ),
    
    -- Create country-level aggregates for Total Group calculations
    last12_month_country AS (
        SELECT
            id_client,
            COUNTRY_KEY
        FROM last12_month
        GROUP BY 1, 2
    ),

    previous12_month_country AS (
        SELECT 
            id_client,
            COUNTRY_KEY
        FROM previous12_month
        GROUP BY 1, 2
    ),
    
    -- Calculate chain-level stable clients
    stable_clients_chain AS (
        SELECT  
            a.CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_constants
        FROM previous12_month a
        INNER JOIN last12_month b
          ON a.id_client = b.id_client
          AND a.CHAIN_TYPE_DESC = b.CHAIN_TYPE_DESC
          AND a.COUNTRY_KEY = b.COUNTRY_KEY
        GROUP BY 1, 2
    ),
    
    -- Calculate Total Group stable clients
    stable_clients_total AS (
        SELECT  
            'Total Group' AS CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_constants
        FROM previous12_month_country a
        INNER JOIN last12_month_country b
          ON a.id_client = b.id_client
          AND a.COUNTRY_KEY = b.COUNTRY_KEY
        GROUP BY 2
    ),
    
    -- Combine chain and total levels
    stable_clients AS (
        SELECT * FROM stable_clients_chain
        UNION ALL
        SELECT * FROM stable_clients_total
    ),
    
    -- Calculate chain-level new clients
    New_clients_chain AS (
        SELECT 
            a.CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_gagnes
        FROM last12_month a
        WHERE NOT EXISTS (
            SELECT 1
            FROM previous12_month b
            WHERE a.id_client = b.id_client
              AND a.CHAIN_TYPE_DESC = b.CHAIN_TYPE_DESC
              AND a.COUNTRY_KEY = b.COUNTRY_KEY
        )
        GROUP BY 1, 2
    ),
    
    -- Calculate Total Group new clients
    New_clients_total AS (
        SELECT 
            'Total Group' AS CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_gagnes
        FROM last12_month_country a
        WHERE NOT EXISTS (
            SELECT 1
            FROM previous12_month_country b
            WHERE a.id_client = b.id_client
              AND a.COUNTRY_KEY = b.COUNTRY_KEY
        )
        GROUP BY 2
    ),
    
    -- Combine chain and total levels
    New_clients AS (
        SELECT * FROM New_clients_chain
        UNION ALL
        SELECT * FROM New_clients_total
    ),
    
    -- Calculate chain-level lost clients
    Lost_clients_chain AS (
        SELECT 
            a.CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_perdus
        FROM previous12_month a
        WHERE NOT EXISTS (
            SELECT 1
            FROM last12_month b
            WHERE a.id_client = b.id_client
              AND a.CHAIN_TYPE_DESC = b.CHAIN_TYPE_DESC
              AND a.COUNTRY_KEY = b.COUNTRY_KEY
        )
        GROUP BY 1, 2
    ),
    
    -- Calculate Total Group lost clients
    Lost_clients_total AS (
        SELECT 
            'Total Group' AS CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_perdus
        FROM previous12_month_country a
        WHERE NOT EXISTS (
            SELECT 1
            FROM last12_month_country b
            WHERE a.id_client = b.id_client
              AND a.COUNTRY_KEY = b.COUNTRY_KEY
        )
        GROUP BY 2
    ),
    
    -- Combine chain and total levels
    Lost_clients AS (
        SELECT * FROM Lost_clients_chain
        UNION ALL
        SELECT * FROM Lost_clients_total
    )
    
    SELECT 
        a.month AS Mois,
        a.COUNTRY_KEY AS Country,
        COALESCE(a.CHAIN_TYPE_DESC, 'Total Group') AS Enseigne,
        
        a.total_sales AS CA_Tous_Clients,
        CAST(NULL AS FLOAT64) AS CA_Tous_Clients_local,
        a.sales_cardholders AS CA_Porteurs,
        CAST(NULL AS FLOAT64) AS CA_Porteurs_local,
        SAFE_DIVIDE(a.sales_cardholders, a.total_sales) AS Taux_CA_encarte,
        
        a.nb_trx AS Nb_transactions_Tous_Clients,
        a.nb_trx_cardholders AS Nb_transactions_porteurs,
        SAFE_DIVIDE(a.nb_trx_cardholders, a.nb_trx) AS Taux_transactions_encartees,
        
        a.nb_cardholders AS Nb_foyers,
        SAFE_DIVIDE(a.nb_trx_cardholders, a.nb_cardholders) AS Frequence_porteurs,
        SAFE_DIVIDE(a.sales_cardholders, a.nb_trx_cardholders) AS Panier_moyen_Porteurs,
        
        CAST(a.sales_qty AS INT64) AS nb_articles,
        CAST(a.sales_qty_carholders AS INT64) AS nb_articles_porteur,
        
        a.Families,
        a.Seniors,
        
        a.sales_promo AS CA_promo,
        CAST(NULL AS FLOAT64) AS CA_promo_local,
        
        all_c.Nb_foyers_constants,
        new_c.Nb_foyers_gagnes,
        lost_c.Nb_foyers_perdus
        
    FROM data_agg a
    LEFT JOIN stable_clients all_c 
      ON all_c.CHAIN_TYPE_DESC = COALESCE(a.CHAIN_TYPE_DESC, 'Total Group') 
     AND all_c.COUNTRY_KEY = a.COUNTRY_KEY
    LEFT JOIN New_clients new_c 
      ON new_c.CHAIN_TYPE_DESC = COALESCE(a.CHAIN_TYPE_DESC, 'Total Group') 
     AND new_c.COUNTRY_KEY = a.COUNTRY_KEY
    LEFT JOIN Lost_clients lost_c 
      ON lost_c.CHAIN_TYPE_DESC = COALESCE(a.CHAIN_TYPE_DESC, 'Total Group') 
     AND lost_c.COUNTRY_KEY = a.COUNTRY_KEY
     
    WHERE a.month IS NOT NULL
      AND a.COUNTRY_KEY IS NOT NULL
    """
