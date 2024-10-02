sql_query = """
CREATE TABLE grouped_capa_produitstarif AS 
SELECT 
    SUBSTRING(cpt."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
    SUM(cpt."PRODUITS DE L'EXERCICE") as sum_groupe_i__produits_de_la_tarification                 
FROM 
    capa_produitstarif cpt  
GROUP BY 1
"""
