sql_query = """
CREATE TABLE sign_HDF AS 
SELECT IIF(LENGTH("FINESS/RPPS") = 8, '0' || "FINESS/RPPS", "FINESS/RPPS") as finess, 
       COUNT(*) as nb_signa 
FROM reclamations_{param_N} 
WHERE signalement = 'Oui' 
  AND "FINESS/RPPS" IS NOT NULL 
GROUP BY 1;
"""