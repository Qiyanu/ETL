from google.cloud import bigquery

def create_italy_query(bq_ops, temp_table_id):
    """Create BigQuery SQL for Italy data with corrected Total Group retention metrics."""
    # Get excluded chain types and prepare parameters
    excluded_chains = bq_ops.config.get("EXCLUDED_CHAIN_TYPES", [])
    excluded_chain_params = []
    
    # Create parameter placeholders for excluded chains
    for i, chain in enumerate(excluded_chains):
        excluded_chain_params.append(f"@excluded_chain_{i}")
    
    excluded_chains_sql = ', '.join(excluded_chain_params)
    
    # Get country mappings from config
    country_mapping = bq_ops.config.get("COUNTRY_MAPPING", {"ITA": "IT", "ESP": "SP"})
    
    # Generate dynamic CASE statement for country mapping
    country_mapping_cases = []
    for source, target in country_mapping.items():
        country_mapping_cases.append(f"WHEN line.COUNTRY_KEY = '{source}' THEN '{target}'")
    
    country_mapping_sql = "\n                ".join(country_mapping_cases)
    if not country_mapping_sql:
        country_mapping_sql = "ELSE line.COUNTRY_KEY"  # Default fallback
    
    # Updated Italy query with dynamic country mapping and parameterized excluded chains
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
    
    -- Client retention calculations
    last12_month AS (
        SELECT
            CASE WHEN card.CARD_KEY <> '-1' THEN card.ACCOUNT_KEY END AS id_client,
            CASE 
                {country_mapping_sql}
                ELSE line.COUNTRY_KEY 
            END AS country_key,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce'
                ELSE store.CHAIN_TYPE_DESC 
            END AS chain_type_desc
        FROM `{bq_ops.config.get("SOURCE_LINE_TABLE")}` AS line
        LEFT JOIN `{bq_ops.config.get("SOURCE_HEADER_TABLE")}` AS header
          ON line.DATE_KEY = header.DATE_KEY
         AND line.COUNTRY_KEY = header.COUNTRY_KEY
         AND line.SITE_KEY = header.SITE_KEY
         AND line.TRX_KEY = header.TRX_KEY
         AND line.TRX_TYPE = header.TRX_TYPE
         AND line.rec_src = header.rec_src
        LEFT JOIN `{bq_ops.config.get("SOURCE_CARD_TABLE")}` AS card
          ON header.CARD_KEY = card.CARD_KEY
        INNER JOIN `{bq_ops.config.get("SOURCE_SITE_TABLE")}` AS site
          ON header.SITE_KEY = site.site_key
         AND header.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config.get("SOURCE_STORE_TABLE")}` AS store
          ON site.MAIN_SITE_KEY = store.STORE_KEY
         AND site.COUNTRY_KEY = store.COUNTRY_KEY

        WHERE DATE_TRUNC(line.date_key, MONTH)
              BETWEEN DATE_SUB(@theMonth, INTERVAL 11 MONTH) AND @theMonth
          AND store.chain_type_key != "CAC"
          AND line.COUNTRY_KEY = 'ITA'
          AND store.CHAIN_TYPE_DESC NOT IN ({excluded_chains_sql})
          AND card.CARD_KEY <> '-1'
        GROUP BY 1, 2, 3
    ),

    previous12_month AS (
        SELECT
            CASE WHEN card.CARD_KEY <> '-1' THEN card.ACCOUNT_KEY END AS id_client,
            CASE 
                {country_mapping_sql}
                ELSE line.COUNTRY_KEY 
            END AS country_key,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce'
                ELSE store.CHAIN_TYPE_DESC
            END AS chain_type_desc
        FROM `{bq_ops.config.get("SOURCE_LINE_TABLE")}` AS line
        LEFT JOIN `{bq_ops.config.get("SOURCE_HEADER_TABLE")}` AS header
          ON line.DATE_KEY = header.DATE_KEY
         AND line.COUNTRY_KEY = header.COUNTRY_KEY
         AND line.SITE_KEY = header.SITE_KEY
         AND line.TRX_KEY = header.TRX_KEY
         AND line.TRX_TYPE = header.TRX_TYPE
         AND line.rec_src = header.rec_src
        LEFT JOIN `{bq_ops.config.get("SOURCE_CARD_TABLE")}` AS card
          ON header.CARD_KEY = card.CARD_KEY
        INNER JOIN `{bq_ops.config.get("SOURCE_SITE_TABLE")}` AS site
          ON line.SITE_KEY = site.site_key
         AND line.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config.get("SOURCE_STORE_TABLE")}` AS store
          ON site.MAIN_SITE_KEY = store.STORE_KEY
         AND site.COUNTRY_KEY = store.COUNTRY_KEY

        WHERE DATE_TRUNC(line.date_key, MONTH)
              BETWEEN DATE_SUB(@theMonth, INTERVAL 23 MONTH) 
                  AND DATE_SUB(@theMonth, INTERVAL 12 MONTH)
          AND store.chain_type_key != "CAC"
          AND line.COUNTRY_KEY = 'ITA'
          AND store.CHAIN_TYPE_DESC NOT IN ({excluded_chains_sql})
          AND card.CARD_KEY <> '-1'
        GROUP BY 1, 2, 3
    ),
    
    -- Create country-level aggregates for Total Group calculations
    last12_month_country AS (
        SELECT
            id_client,
            country_key
        FROM last12_month
        GROUP BY 1, 2
    ),

    previous12_month_country AS (
        SELECT 
            id_client,
            country_key
        FROM previous12_month
        GROUP BY 1, 2
    ),
    
    -- Calculate chain-level stable clients
    stable_clients_chain AS (
        SELECT
            a.chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_constants
        FROM previous12_month a
        INNER JOIN last12_month b
          ON a.id_client = b.id_client
         AND a.chain_type_desc = b.chain_type_desc
         AND a.country_key = b.country_key
        GROUP BY 1, 2
    ),

    -- Calculate Total Group stable clients
    stable_clients_total AS (
        SELECT
            'Total Group' AS chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_constants
        FROM previous12_month_country a
        INNER JOIN last12_month_country b
          ON a.id_client = b.id_client
         AND a.country_key = b.country_key
        GROUP BY 2
    ),

    -- Combine chain and total levels
    stable_clients AS (
        SELECT * FROM stable_clients_chain
        UNION ALL
        SELECT * FROM stable_clients_total
    ),

    -- Calculate chain-level new clients
    new_clients_chain AS (
        SELECT
            a.chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_gagnes
        FROM last12_month a
        WHERE NOT EXISTS (
            SELECT 1
            FROM previous12_month b
            WHERE a.id_client = b.id_client
              AND a.chain_type_desc = b.chain_type_desc
              AND a.country_key = b.country_key
        )
        GROUP BY 1, 2
    ),

    -- Calculate Total Group new clients
    new_clients_total AS (
        SELECT
            'Total Group' AS chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_gagnes
        FROM last12_month_country a
        WHERE NOT EXISTS (
            SELECT 1
            FROM previous12_month_country b
            WHERE a.id_client = b.id_client
              AND a.country_key = b.country_key
        )
        GROUP BY 2
    ),

    -- Combine chain and total levels
    new_clients AS (
        SELECT * FROM new_clients_chain
        UNION ALL
        SELECT * FROM new_clients_total
    ),

    -- Calculate chain-level lost clients
    lost_clients_chain AS (
        SELECT
            a.chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_perdus
        FROM previous12_month a
        WHERE NOT EXISTS (
            SELECT 1
            FROM last12_month b
            WHERE a.id_client = b.id_client
              AND a.chain_type_desc = b.chain_type_desc
              AND a.country_key = b.country_key
        )
        GROUP BY 1, 2
    ),

    -- Calculate Total Group lost clients
    lost_clients_total AS (
        SELECT
            'Total Group' AS chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_perdus
        FROM previous12_month_country a
        WHERE NOT EXISTS (
            SELECT 1
            FROM last12_month_country b
            WHERE a.id_client = b.id_client
              AND a.country_key = b.country_key
        )
        GROUP BY 2
    ),

    -- Combine chain and total levels
    lost_clients AS (
        SELECT * FROM lost_clients_chain
        UNION ALL
        SELECT * FROM lost_clients_total
    )
    
    -- Final aggregation with calculated metrics
    SELECT
        data_agg.month AS Mois,
        data_agg.country_key AS Country,
        COALESCE(data_agg.chain_type_desc, 'Total Group') AS Enseigne,
        
        -- Raw metrics
        data_agg.CA_Tous_Clients,
        data_agg.CA_Tous_Clients_local,
        data_agg.CA_Porteurs,
        data_agg.CA_Porteurs_local,
        SAFE_DIVIDE(data_agg.CA_Porteurs, data_agg.CA_Tous_Clients) AS Taux_CA_encarte,
        
        data_agg.Nb_transactions_Tous_Clients,
        data_agg.Nb_transactions_porteurs,
        SAFE_DIVIDE(data_agg.Nb_transactions_porteurs, data_agg.Nb_transactions_Tous_Clients) AS Taux_transactions_encartees,
        data_agg.Nb_foyers,
        SAFE_DIVIDE(data_agg.Nb_transactions_porteurs, data_agg.Nb_foyers) AS Frequence_porteurs,
        SAFE_DIVIDE(data_agg.CA_Porteurs, data_agg.Nb_transactions_porteurs) AS Panier_moyen_Porteurs,
        
        CAST(data_agg.nb_articles AS INT64) AS nb_articles,
        CAST(data_agg.nb_articles_porteur AS INT64) AS nb_articles_porteur,
        
        data_agg.Families,
        data_agg.Seniors,
        
        data_agg.CA_promo,
        data_agg.CA_promo_local,
        
        -- Retention metrics
        sc.nb_foyers_constants,
        nc.nb_foyers_gagnes,
        lc.nb_foyers_perdus
        
    FROM data_agg
    LEFT JOIN stable_clients sc
        ON sc.chain_type_desc = COALESCE(data_agg.chain_type_desc, 'Total Group')
       AND sc.country_key = data_agg.country_key
    LEFT JOIN new_clients nc
        ON nc.chain_type_desc = COALESCE(data_agg.chain_type_desc, 'Total Group')
       AND nc.country_key = data_agg.country_key
    LEFT JOIN lost_clients lc
        ON lc.chain_type_desc = COALESCE(data_agg.chain_type_desc, 'Total Group')
       AND lc.country_key = data_agg.country_key
    
    WHERE data_agg.month IS NOT NULL
      AND data_agg.country_key IS NOT NULL
    """
    
    # Create parameters list for the query
    params = [bigquery.ScalarQueryParameter("theMonth", "DATE", None)]  # Main parameter for @theMonth
    
    # Add parameters for excluded chains
    for i, chain in enumerate(excluded_chains):
        params.append(bigquery.ScalarQueryParameter(f"excluded_chain_{i}", "STRING", chain))
    
    # Return both the query and the parameters
    return query, params