sql_query = """
    CREATE TABLE correspondance AS 
    SELECT SUBSTRING(cecpp."FINESS - RS ET", 1, 9) as finess,
           cecpp.CADRE 
    FROM choix_errd_ca_pa_ph cecpp 
    LEFT JOIN doublons_errd_ca dou on SUBSTRING(dou.finess, 1, 9) = SUBSTRING(cecpp."FINESS - RS ET", 1, 9) AND cecpp.cadre != 'ERRD' 
    WHERE dou.finess IS NULL
    """
