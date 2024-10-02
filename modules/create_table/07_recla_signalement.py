sql_query = """
CREATE TABLE recla_signalement AS 
SELECT 
    tfc.finess, 
    s.nb_signa, 
    tr.nb_recla, 
    s."Nombre d'EI sur la période 36mois", 
    s.NB_EIGS, 
    s.NB_EIAS, 
    s."Somme EI + EIGS + EIAS sur la période", 
    s."nb EI/EIG : Acte de prévention", 
    s."nb EI/EIG : Autre prise en charge", 
    s."nb EI/EIG : Chute", 
    s."nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)", 
    s."nb EI/EIG : Dispositif médical", 
    s."nb EI/EIG : Fausse route", 
    s."nb EI/EIG : Infection associée aux soins (IAS) hors ES", 
    s."nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)", 
    s."nb EI/EIG : Parcours/Coopération interprofessionnelle", 
    s."nb EI/EIG : Prise en charge chirurgicale", 
    s."nb EI/EIG : Prise en charge diagnostique", 
    s."nb EI/EIG : Prise en charge en urgence", 
    s."nb EI/EIG : Prise en charge médicamenteuse", 
    s."nb EI/EIG : Prise en charge des cancers", 
    s."nb EI/EIG : Prise en charge psychiatrique", 
    s."nb EI/EIG : Suicide", 
    s."nb EI/EIG : Tentative de suicide", 
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
FROM tfiness_clean tfc 
LEFT JOIN table_recla tr ON tr.finess = tfc.finess 
LEFT JOIN igas i ON i.finess = tfc.finess 
LEFT JOIN sign s ON s.finess = tfc.finess;
"""
