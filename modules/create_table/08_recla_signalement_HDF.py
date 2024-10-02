sql_query = """
CREATE TABLE recla_signalement_HDF AS 
SELECT 
    tfc.finess, 
    sh.nb_signa, 
    tr.nb_recla, 
    i."Hôtellerie-locaux-restauration", 
    i."Problème d?organisation ou de fonctionnement de l?établissement ou du service", 
    i."Problème de qualité des soins médicaux", 
    i."Problème de qualité des soins paramédicaux", 
    i."Recherche d?établissement ou d?un professionnel", 
    i."Mise en cause attitude des professionnels", 
    i."Informations et droits des usagers", 
    i."Facturation et honoraires", 
    i."Santé-environnementale", 
    i."Activités d?esthétique réglementées", 
    i."A renseigner", 
    i."COVID-19" 
FROM 
    tfiness_clean tfc 
    LEFT JOIN table_recla tr ON tr.finess = tfc.finess 
    LEFT JOIN igas i ON i.finess = tfc.finess 
    LEFT JOIN sign_HDF sh ON sh.finess = tfc.finess
"""
