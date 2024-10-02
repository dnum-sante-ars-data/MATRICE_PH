sql_query = """
CREATE TABLE grouped_errd_charges AS 
SELECT 
    SUBSTRING(ep."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
    SUM(ep."Charges d'exploitation") as sum_charges_dexploitation 
FROM 
    errd_charges ep 
GROUP BY 1
"""
