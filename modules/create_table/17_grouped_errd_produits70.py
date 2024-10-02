sql_query = """
    CREATE TABLE grouped_errd_produits70 AS 
    SELECT SUBSTRING(ep2."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
           SUM(ep2.unnamed_1) as sum_produits70 
    FROM errd_produits70 ep2 
    GROUP BY 1
    """
