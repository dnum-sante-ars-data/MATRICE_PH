sql_query = """
CREATE TABLE clean_occupation_N_2 AS 
SELECT 
    IIF(LENGTH(o3.finess) = 8, '0' || o3.finess, o3.finess) as finess, 
    o3.taux_occ_{param_N_2}, 
    o3.nb_lits_autorises_installes, 
    o3.nb_lits_occ_{param_N_2}, 
    o3.taux_occ_trimestre3 
FROM 
    occupation_{param_N_2} o3
"""
