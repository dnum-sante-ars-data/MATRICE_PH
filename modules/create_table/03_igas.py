sql_query = """
    CREATE TABLE igas AS 
    SELECT 
	IIF(LENGTH(se."FINESS/RPPS" )= 8, '0'|| se."FINESS/RPPS", se."FINESS/RPPS") as finess, 
	SUM(IIF(se."Motifs IGAS" like '%Hôtellerie-locaux-restauration%',1,0)) as "Hôtellerie-locaux-restauration",
	SUM(IIF(se."Motifs IGAS" like '%Problème d?organisation ou de fonctionnement de l?établissement ou du service%',1,0)) as "Problème d?organisation ou de fonctionnement de l?établissement ou du service",
	SUM(IIF(se."Motifs IGAS" like '%Problème de qualité des soins médicaux%',1,0)) as "Problème de qualité des soins médicaux",
	SUM(IIF(se."Motifs IGAS" like '%Problème de qualité des soins paramédicaux%',1,0)) as "Problème de qualité des soins paramédicaux",
	SUM(IIF(se."Motifs IGAS" like '%Recherche d?établissement ou d?un professionnel%',1,0)) as "Recherche d?établissement ou d?un professionnel",
	SUM(IIF(se."Motifs IGAS" like '%Mise en cause attitude des professionnels%',1,0)) as "Mise en cause attitude des professionnels",
	SUM(IIF(se."Motifs IGAS" like '%Informations et droits des usagers%',1,0)) as "Informations et droits des usagers",
	SUM(IIF(se."Motifs IGAS" like '%Facturation et honoraires%',1,0)) as "Facturation et honoraires",
	SUM(IIF(se."Motifs IGAS" like '%Santé-environnementale%',1,0)) as "Santé-environnementale",
	SUM(IIF(se."Motifs IGAS" like '%Activités d?esthétique réglementées%',1,0)) as "Activités d?esthétique réglementées",
	SUM(IIF(se."Motifs IGAS" like '%A renseigner%',1,0)) as "A renseigner",
	SUM(IIF(se."Motifs IGAS" like '%COVID-19%',1,0)) as "COVID-19"
    FROM reclamations_{param_N} se
    WHERE 
	(se.Signalement = 'Non' or se.Signalement IS NULL)
	AND se."FINESS/RPPS"  IS NOT NULL
    GROUP BY 1
    """