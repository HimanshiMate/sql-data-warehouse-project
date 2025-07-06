/* this table is dimension customer table because it give detail about the table, if you are creating new dimension you need primary key for dimension.
if you don't have PK in dimension table then you need to generate PK in DWH those key is known as "SURROGATE KEY(sys-generate unique identifier)".
There is different way to generate surrogate key 1. DDL- based generation
                                                 2.Query based using Window function(row_number) */


IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
      ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, --surrogate key
      ci.cst_id AS Customer_id,
      ci.cst_key AS Customer_number,
      ci.cst_firstname AS First_name,
      ci.cst_lastname AS Last_name,
      ca.bdate AS Birthdate,
      la.cntry AS Country,
      ci.cst_marital_status AS Marital_status,
      CASE WHEN ci.cst_gndr !='NA' THEN ci.cst_gndr --crm is the master for gender info
           ELSE COALESCE(ca.gen ,'NA') --go to erp
      END AS Gender,	 
      ci.cst_create_date AS Create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_aZ12 ca
ON ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_az12 la
ON ci.cst_key= la.cid
GO

--SELECT * FROM gold.dim_customers

--====================================CREATE DIMENSION: FOR PRODUCT===============

IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO
  
CREATE VIEW gold.dim_product AS
SELECT 
      ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, --Surrogate key
      pn.prd_id AS product_id,
      pn.prd_key AS product_number,
      pn.prd_nm AS product_name,
      pn.cat_id AS category_id,
      pc.cat AS category,
      pc.subcat AS subcategory,
      pc.maintenance AS maintenance,
      pn.prd_cost AS Cost,
      pn.prd_line AS product_line,
      pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL; --filter out all historical data
GO

--SELECT * FROM gold.dim_product

--==========================CREATE DIMENSION: FOR FACT SALES=================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
  
CREATE VIEW gold.fact_sales AS
SELECT
      sd.sls_ord_num AS order_number,
      pr.product_key,
      cu.customer_key,
      sd.sls_order_dt as order_date,
      sd.sls_ship_dt as shipping_date,
      sd.sls_due_dt as due_date,
      sd.sls_sales as sales_amount,
      sd.sls_quantity as quantity,
      sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
GO
