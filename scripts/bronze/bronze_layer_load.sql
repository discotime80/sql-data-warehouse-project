CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	start_batch_time TIMESTAMP;
	end_batch_time TIMESTAMP;
BEGIN
	start_batch_time := NOW();
	start_time := NOW();
		TRUNCATE TABLE bronze.crm_cust_info;
		COPY bronze.crm_cust_info
		FROM '/tmp/cust_info.csv'
		DELIMITER ','
		CSV HEADER;
	end_time := NOW();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	start_time := NOW();	
		TRUNCATE TABLE bronze.crm_prd_info;
		COPY bronze.crm_prd_info
		FROM '/tmp/prd_info.csv'
		DELIMITER ','
		CSV HEADER;
	end_time := NOW();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));	

	start_time := NOW();	
		TRUNCATE TABLE bronze.crm_sales_details;
		COPY bronze.crm_sales_details
		FROM '/tmp/sales_details.csv'
		DELIMITER ','
		CSV HEADER;
	end_time := NOW();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	start_time := NOW();
		TRUNCATE TABLE bronze.erp_cust_az12;
		COPY bronze.erp_cust_az12
		FROM '/tmp/CUST_AZ12.csv'
		DELIMITER ','
		CSV HEADER;
	end_time := NOW();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));	

	start_time := NOW();	
		TRUNCATE TABLE bronze.erp_loc_a101;
		COPY bronze.erp_loc_a101
		FROM '/tmp/LOC_A101.csv'
		DELIMITER ','
		CSV HEADER;
	end_time := NOW();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	start_time := NOW();		
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		COPY bronze.erp_px_cat_g1v2
		FROM '/tmp/PX_CAT_G1V2.csv'
		DELIMITER ','
		CSV HEADER;
	end_time := NOW();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	end_batch_time := NOW();
	RAISE NOTICE 'Total load duration: % seconds', EXTRACT(EPOCH FROM (end_batch_time - start_batch_time));
	
EXCEPTION 
	WHEN OTHERS THEN 
		RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
END;
$$;

CALL bronze.load_bronze();
