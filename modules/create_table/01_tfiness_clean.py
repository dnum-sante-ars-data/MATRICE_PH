
sql_query = """
CREATE TABLE tfiness_clean AS 
SELECT 
    IIF(LENGTH(tf_with.finess) = 8, '0' || tf_with.finess, tf_with.finess) AS finess, 
    tf_with.categ_lib, 
    tf_with.categ_code, 
    tf_with.rs, 
    IIF(LENGTH(tf_with.ej_finess) = 8, '0' || tf_with.ej_finess, tf_with.ej_finess) AS ej_finess, 
    tf_with.ej_rs, 
    tf_with.statut_jur_lib, 
    IIF(tf_with.adresse_num_voie IS NULL, '', 
        SUBSTRING(CAST(tf_with.adresse_num_voie AS TEXT), 1, LENGTH(CAST(tf_with.adresse_num_voie AS TEXT)) - 2) || ' ') || 
        IIF(tf_with.adresse_comp_voie IS NULL, '', tf_with.adresse_comp_voie || ' ') || 
        IIF(tf_with.adresse_type_voie IS NULL, '', tf_with.adresse_type_voie || ' ') || 
        IIF(tf_with.adresse_nom_voie IS NULL, '', tf_with.adresse_nom_voie || ' ') || 
        IIF(tf_with.adresse_lieuditbp IS NULL, '', tf_with.adresse_lieuditbp || ' ') || 
        IIF(tf_with.adresse_lib_routage IS NULL, '', tf_with.adresse_lib_routage) AS adresse, 
    CAST(adresse_code_postal AS INTEGER) AS adresse_code_postal, 
    tf_with.com_code 
FROM 't-finess' tf_with 
WHERE tf_with.categ_code IN (238,190,189,395,246,249,188,437,253,382,252,192,196,183,195,402,255,445,446,464);
"""