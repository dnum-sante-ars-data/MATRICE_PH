sql_query = """
CREATE TABLE grouped_caph_produitsencaiss AS 
SELECT 
    SUBSTRING(cch4."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
    SUM(cch4."Produits d'exploitation") as sum_produits_dexploitation 
FROM 
    caph_produitsencaiss cch4 
GROUP BY 1
"""
