

-- Create View 'dim_product'
CREATE VIEW gold.dim_product AS 
    SELECT 
        ROW_NUMBER() OVER(ORDER BY cp.prd_start_dt, cp.prd_id) AS product_key,
        cp.prd_id       AS product_id,
        cp.sls_prd_key  AS product_number,
        cp.prd_nm       AS prdocut_name,
        cp.cat_id       AS categroy_id,
        ep.cat          AS category,
        ep.subcat       AS subcategory,
        ep.maintenance,
        cp.prd_cost     AS cost,
        cp.prd_line     AS product_line,
        cp.prd_start_dt AS start_date
    FROM silver.crm_prd_info cp
    LEFT JOIN silver.epr_px_cat_g1v2 ep
        ON cp.cat_id = ep.id
    WHERE prd_end_dt IS NULL;


-- Create View 'dim_customer'
CREATE VIEW gold.dim_customer AS 
    SELECT 
        ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname  AS last_name,
        ci.cst_marital_status  AS marital_status,
        CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
             ELSE COALESCE(eb.gen, 'N/A')
        END AS gender,
        el.cntry AS country,
        eb.bdate AS birthdate,
        ci.cst_create_date AS create_date
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.epr_cust_az12 eb
      ON ci.cst_key = eb.cid
    LEFT JOIN silver.epr_loc_a101  el
      ON ci.cst_key = el.cid;


-- Create View 'fact_sales'
CREATE VIEW gold.fact_sales AS
    SELECT 
        cs.sls_ord_num AS order_number,
        dp.product_key,
        dc.customer_key,
        cs.sls_order_dt AS order_date,
        cs.sls_ship_dt  AS ship_date,
        cs.sls_due_dt   AS due_date,
        cs.sls_sales    AS sales_amount,
        cs.sls_quantity AS quantity,
        cs.sls_price    AS price
    FROM silver.crm_sales_details cs
    LEFT JOIN gold.dim_customer dc
        ON cs.sls_cust_id = dc.customer_id
    LEFT JOIN gold.dim_product dp
        ON cs.sls_prd_key = dp.product_number;
