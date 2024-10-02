sql_query = """
    CREATE TABLE inspections AS 
    SELECT finess, 
           SUM(IIF(realise = 'oui', 1, 0)) AS 'ICE {param_N_1} (réalisé)', 
           SUM(IIF(realise = 'oui' AND CTRL_PL_PI = 'Contrôle sur place', 1, 0)) AS 'Inspection SUR SITE {param_N_1} - Déjà réalisée', 
           SUM(IIF(realise = 'oui' AND CTRL_PL_PI = 'Contrôle sur pièces', 1, 0)) AS 'Controle SUR PIECE {param_N_1} - Déjà réalisé', 
           SUM(IIF(programme = 'oui', 1, 0)) AS 'Inspection / contrôle Programmé {param_N}' 
    FROM (
        SELECT finess, 
               "Identifiant de la mission", 
               "Date provisoire Visite", 
               "Date réelle Visite", 
               CTRL_PL_PI, 
               IIF(CAST(SUBSTR("Date réelle Visite", 7, 4) || SUBSTR("Date réelle Visite", 4, 2) || SUBSTR("Date réelle Visite", 1, 2) AS INTEGER) <= 20231231, 'oui', '') AS realise, 
               IIF(CAST(SUBSTR("Date réelle Visite", 7, 4) || SUBSTR("Date réelle Visite", 4, 2) || SUBSTR("Date réelle Visite", 1, 2) AS INTEGER) > 20231231 AND 
                   CAST(SUBSTR("Date provisoire Visite", 7, 4) || SUBSTR("Date provisoire Visite", 4, 2) || SUBSTR("Date provisoire Visite", 1, 2) AS INTEGER) > 20231231, 'oui', '') AS programme 
        FROM (
            SELECT *, 
                   IIF(LENGTH("Code FINESS") = 8, '0' || "Code FINESS", "Code FINESS") AS finess, 
                   "Modalité d'investigation" AS CTRL_PL_PI 
            FROM HELIOS_SICEA_MISSIONS_{param_N} 
            WHERE CAST(SUBSTR("Date réelle Visite", 7, 4) || SUBSTR("Date réelle Visite", 4, 2) || SUBSTR("Date réelle Visite", 1, 2) AS INTEGER) >= 20230101 
                  AND "Code FINESS" IS NOT NULL
        ) brut 
        GROUP BY finess, "Identifiant de la mission", "Date provisoire Visite", "Date réelle Visite", CTRL_PL_PI
    ) brut_agg 
    GROUP BY finess;
    """