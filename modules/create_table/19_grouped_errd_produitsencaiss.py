sql_query = """
CREATE TABLE grouped_errd_produitsencaiss AS 
SELECT 
    SUBSTRING(ep3."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
    SUM(ep3."Produits d'exploitation") as sum_produits_dexploitation 
FROM 
    errd_produitsencaiss ep3 
GROUP BY 1
"""
