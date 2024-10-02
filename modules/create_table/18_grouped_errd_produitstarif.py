sql_query = """
CREATE TABLE grouped_errd_produitstarif AS 
SELECT 
    SUBSTRING(ep."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
    SUM(ep."GROUPE I : PRODUITS DE LA TARIFICATION") as sum_groupe_i__produits_de_la_tarification 
FROM 
    errd_produitstarif ep 
GROUP BY 1
"""
