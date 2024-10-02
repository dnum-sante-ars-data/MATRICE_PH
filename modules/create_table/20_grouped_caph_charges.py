sql_query = """
CREATE TABLE grouped_caph_charges AS 
SELECT 
    SUBSTRING(cch."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
    SUM(cch."Charges d'exploitation") as sum_charges_dexploitation 
FROM 
    caph_charges cch  
GROUP BY 1
"""
