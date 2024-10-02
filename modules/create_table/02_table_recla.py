sql_query = """
CREATE TABLE table_recla AS 
SELECT IIF(LENGTH(se."FINESS/RPPS") = 8, '0'|| se."finess/rpps", se."finess/rpps") as finess, 
       COUNT(*) as nb_recla 
FROM RECLAMATIONs_{param_N}  se 
WHERE se."finess/rpps" IS NOT NULL AND (se.Signalement = 'Non' or se.Signalement IS NULL) 
GROUP BY 1
"""