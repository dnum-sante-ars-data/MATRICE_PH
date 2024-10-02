sql_query = """
CREATE TABLE clean_hebergement AS 
SELECT 
    IIF(LENGTH(h.finesset) = 8, '0' || h.finesset, h.finesset) as finess, 
    h."prixHebPermCs" 
FROM 
    hebergement h
"""
