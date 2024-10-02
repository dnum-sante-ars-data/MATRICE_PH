sql_query = """
CREATE TABLE grouped_caph_produits70 AS 
SELECT 
    SUBSTRING(cch3."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
    SUM(cch3.unnamed_1) as sum_produits70 
FROM 
    caph_produits70 cch3 
GROUP BY 1
"""
