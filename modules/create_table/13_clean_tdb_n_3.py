sql_query = """
CREATE TABLE clean_tdb_n_3 AS 
SELECT 
    IIF(LENGTH("finess géographique") = 8, '0' || "finess géographique", "finess géographique") as finess, 
    * 
FROM 
    "export-tdbesms-{param_N_3}-region-agg"
"""
