sql_query = """
CREATE TABLE grouped_caph_produitstarif AS 
SELECT 
    SUBSTRING(cch2."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
    SUM(cch2."GROUPE I : PRODUITS DE LA TARIFICATION") as sum_groupe_i__produits_de_la_tarification 
FROM 
    caph_produitstarif cch2 
GROUP BY 1
"""
