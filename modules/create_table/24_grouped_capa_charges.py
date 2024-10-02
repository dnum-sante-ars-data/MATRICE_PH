sql_query = """
CREATE TABLE grouped_capa_charges AS 
SELECT 
    SUBSTRING(cc."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
    SUM(cc."CHARGES D'EXPLOITATION") as sum_charges_dexploitation 
FROM 
    capa_charges cc  
GROUP BY 1
"""
