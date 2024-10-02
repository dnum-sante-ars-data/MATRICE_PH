sql_query = """
CREATE TABLE clean_capacite_totale_auto AS 
SELECT 
    IIF(LENGTH("Étiquettes de lignes") = 8, '0' || "Étiquettes de lignes", "Étiquettes de lignes") as finess, 
    "Somme de Capacité autorisée totale " as somme_de_capacite_autorisee_totale_ 
FROM 
    capacite_totale_auto
"""
