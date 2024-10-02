 
 
sql_query="""
   CREATE TABLE moyenne_ciblage AS
 SELECT 
    AVG(
        CASE
            WHEN tf.categ_code = 500
                THEN CAST(NULLTOZERO(ce."TOTAL Héberg. Comp. Inter. Places Autorisées") AS INTEGER) + 
                     CAST(NULLTOZERO(ce."TOTAL Accueil de Jour Places Autorisées") AS INTEGER) + 
                     CAST(NULLTOZERO(ce."TOTAL Accueil de nuit Places Autorisées") AS INTEGER)
            ELSE CAST(ccta.somme_de_capacite_autorisee_totale_ AS INTEGER)
        END
    ) AS "Capacité totale autorisée",
            AVG(fp."Type d'activité indifferencié") AS "Type d'activité indifferencié",
        AVG(fp."Protection Juridique") AS "Protection Juridique",
    AVG(fp."Hébergement Complet Internat") AS "Hébergement Complet Internat",
    AVG(fp."Placement Famille d'Accueil") AS "Placement Famille d'Accueil",
    AVG(fp."Accueil temporaire avec hébergement") AS "Accueil temporaire avec hébergement",
    AVG(fp."Prestation en milieu ordinaire") AS "Prestation en milieu ordinaire",
    AVG(fp."Accueil de jour et accompagnement en milieu ordinaire") AS "Accueil de jour et accompagnement en milieu ordinaire",
    AVG(fp."Accueil de Jour") AS "Accueil de Jour",
    AVG(fp."Accueil temporaire (avec et sans hébergement)") AS "Accueil temporaire (avec et sans hébergement)",
    AVG(fp."Semi-Internat") AS "Semi-Internat",
    AVG(fp."Hébergement de Nuit Eclaté") AS "Hébergement de Nuit Eclaté",
    AVG(fp."Internat de Semaine") AS "Internat de Semaine",
    AVG(fp."Externat") AS "Externat",
    AVG(fp."Tous modes d'accueil et d'accompagnement") AS "Tous modes d'accueil et d'accompagnement",
    AVG(fp."Tous modes d'accueil avec hébergement") AS "Tous modes d'accueil avec hébergement",
    AVG(fp."Accueil de Nuit") AS "Accueil de Nuit",
    AVG(fp."Permanence téléphonique") AS "Permanence téléphonique",
    AVG(fp."Accueil temporaire de jour") AS "Accueil temporaire de jour",
    AVG(fp."Consultation Soins Externes") AS "Consultation Soins Externes",
    AVG(fp."Accueil modulable/séquentiel") AS "Accueil modulable/séquentiel",
    AVG(fp."Traitement et Cure Ambulatoire") AS "Traitement et Cure Ambulatoire",
    AVG(fp."Tous modes d'accueil (avec et sans hébergement)") AS "Tous modes d'accueil (avec et sans hébergement)",
    AVG(fp."Equipe mobile de rue") AS "Equipe mobile de rue",
    AVG(fp."Accueil et prise en charge en appartement thérapeutique") AS "Accueil et prise en charge en appartement thérapeutique",
    AVG(fp."Hospitalisation Complète") AS "Hospitalisation Complète",
    AVG(fp."Regroupement Calcules (Annexes XXIV)") AS "Regroupement Calcules (Annexes XXIV)",
    AVG(fp."Aide Judiciaire à la Gestion du Budget Familial") AS "Aide Judiciaire à la Gestion du Budget Familial",
    AVG(fp."Accompagnement Social Personnalisé") AS "Accompagnement Social Personnalisé",
    AVG(fp."Information des Tuteurs Familiaux") AS "Information des Tuteurs Familiaux",
    AVG(fp."Administration") AS "Administration",
    AVG(co3.nb_lits_occ_{param_N_2}) AS "Nombre de résidents au 31/12/{param_N_2}",
    AVG(etra."Nombre total de chambres installées au 31.12") AS "Nombre de places installées au 31/12/{param_N_2}",
    AVG(ROUND(CAST(REPLACE(eira."Taux_plus_10_médics (cip13)", ",", ".") AS FLOAT),2)) AS "Part des résidents ayant plus de 10 médicaments consommés par mois",
        --ROUND(CAST(REPLACE(eira.taux_atu, ",", ".") AS FLOAT),2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
        "" as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
    AVG(CAST(NULLIF(REPLACE(taux_hospit_mco, ',', '.'), '') AS FLOAT)) AS "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
    AVG(CAST(NULLIF(REPLACE(taux_hospit_had, ',', '.'), '') AS FLOAT)) AS "Taux de recours à l'HAD des résidents d'EHPAD",
    AVG(CAST(ede."deficit ou excédent" AS DECIMAL)) AS "deficit ou excédent {param_N_4}",
    AVG(CAST(ede2."deficit ou excédent" AS DECIMAL)) AS "deficit ou excédent {param_N_3}",
    AVG(CAST(ede3."deficit ou excédent" AS DECIMAL)) AS "deficit ou excédent {param_N_2}",
    "" as "Saisie des indicateurs du TDB MS (campagne {param_N_2})",
    AVG(CAST(d2."Taux d'absentéisme (hors formation) en %" AS DECIMAL)) AS "Taux d'absentéisme {param_N_4}",
    AVG(CAST(etra2."Taux d'absentéisme (hors formation) en %" AS DECIMAL)) AS "Taux d'absentéisme {param_N_3}",
    AVG(CAST(etra."Taux d'absentéisme (hors formation) en %" AS FLOAT)) AS "Taux d'absentéisme {param_N_2}",
    AVG(ROUND(MOY3(d2."Taux d'absentéisme (hors formation) en %" ,etra2."Taux d'absentéisme (hors formation) en %"    , etra."Taux d'absentéisme (hors formation) en %") ,2)) as "Absentéisme moyen sur la période {param_N_4}-{param_N_2}",
    AVG(CAST(d2."Taux de rotation des personnels" AS DECIMAL)) AS "Taux de rotation du personnel titulaire {param_N_4}",
    AVG(CAST(etra2."Taux de rotation des personnels" AS FLOAT)) AS "Taux de rotation du personnel titulaire {param_N_3}",
    AVG(CAST(etra."Taux de rotation des personnels" AS FLOAT)) AS "Taux de rotation du personnel titulaire {param_N_2}",
    AVG(ROUND(MOY3(d2."Taux de rotation des personnels" , etra2."Taux de rotation des personnels" , CAST(etra."Taux de rotation des personnels" as FLOAT)), 2)) as "Rotation moyenne du personnel sur la période {param_N_4}-{param_N_2}",
    AVG(CAST(d2."Taux d'ETP vacants" AS DECIMAL)) AS "ETP vacants {param_N_4}",
    AVG(CAST(etra2."Taux d'ETP vacants" AS DECIMAL)) AS "ETP vacants {param_N_3}",
    AVG(CAST(etra."Taux d'ETP vacants" AS DECIMAL)) AS "ETP vacants {param_N_2}",
    AVG(CAST(etra."Dont taux d'ETP vacants concernant la fonction SOINS" AS DECIMAL)) AS "dont fonctions soins {param_N_2}",
    AVG(CAST(etra."Dont taux d'ETP vacants concernant la fonction SOCIO EDUCATIVE" AS DECIMAL)) AS "dont fonctions socio-éducatives {param_N_2}",
    AVG(CAST(REPLACE(d2."Taux de prestations externes sur les prestations directes", ',', '.') AS DECIMAL)) AS "Taux de prestations externes sur les prestations directes {param_N_4}",
    AVG(CAST(etra2."Taux de prestations externes sur les prestations directes" AS DECIMAL)) AS "Taux de prestations externes sur les prestations directes {param_N_3}",
    AVG(CAST(etra."Taux de prestations externes sur les prestations directes" AS DECIMAL)) AS "Taux de prestations externes sur les prestations directes {param_N_2}",
    AVG(ROUND(MOY3(d2."Taux de prestations externes sur les prestations directes" , etra2."Taux de prestations externes sur les prestations directes" , CAST(etra."Taux de prestations externes sur les prestations directes" as float)) ,2)) as "Taux moyen de prestations externes sur les prestations directes",
    AVG(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions") / d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)) AS "Nombre total d'ETP par usager en {param_N_4}",
    AVG(ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale" + etra2."ETP Autres fonctions") / etra2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)) AS "Nombre total d'ETP par usager en {param_N_3}",
    AVG(ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)) as "Nombre total d'ETP par usager en {param_N_2}",
    ROUND(MOY3(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) , ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) , 
    ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)),2)AS "Nombre moyen d'ETP par usager sur la période {param_N_4}-{param_N_2}",
    AVG(ROUND((cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Médical" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)) as "ETP 'soins' par usager en {param_N_2}",
    AVG(CAST(etra."- Dont nombre d'ETP réels de médecin coordonnateur" AS FLOAT)) AS "dont médecin coordonnateur",
    AVG(ROUND(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT), 2)) as "Total du nombre d'ETP",
    AVG(ROUND((cAST(etra."ETP Services généraux" as FLOAT))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2)) as "Taux de personnel soins",
    AVG(ROUND((cAST(etra."ETP Socio-éducatif" as float))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2)) as "Taux de personnel socio educatif",
    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_intellectuelles_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Déficiences intellectuelles principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Autisme_et_autres_TED_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Autisme et autres TED principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Troubles_du_psychisme_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Troubles du psychisme principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Troubles_du_langage_et_des_apprentissages_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Troubles du langage et des apprentissages principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_auditives_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Déficiences auditives principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_visuelles_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Déficiences visuelles principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_motrices_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Déficiences motrices principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_métaboliques_viscérales_et_nutritionnelles_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Déficiences métaboliques viscérales et nutritionnelles principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Cérébro_lésions_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Cérébro lésions principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Polyhandicap_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Polyhandicap principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Troubles_du_comportement_et_de_la_communication_princiaples(TCC)" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Troubles du comportement et de la communication principales (TCC)",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Diagnostics_en_cours_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Diagnostics en cours principales",

    AVG(
        CASE 
            WHEN co3.nb_lits_occ_{param_N_2} = 0 THEN NULL
            ELSE ROUND(CAST(etra."Autres_types_de_déficiences_principales" AS FLOAT) / co3.nb_lits_occ_{param_N_2}, 2)
        END
    ) AS "Taux de résidents par Autres types de déficiences principales",
	AVG(NULLTOZERO(rs.nb_recla)) AS "Nombre de réclamations sur la période {param_N_3}-{param_N}",
	AVG(NULLTOZERO(ROUND(CAST(rs.nb_recla AS FLOAT) / CAST(ccta.somme_de_capacite_autorisee_totale_ AS FLOAT), 4) * 100)) AS "Rapport réclamations / capacité",
	AVG(NULLTOZERO(rs."Hôtellerie-locaux-restauration")) AS "Recla IGAS : Hôtellerie-locaux-restauration",
	AVG(NULLTOZERO(rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service")) AS "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
	AVG(NULLTOZERO(rs."Problème de qualité des soins médicaux")) AS "Recla IGAS : Problème de qualité des soins médicaux",
	AVG(NULLTOZERO(rs."Problème de qualité des soins paramédicaux")) AS "Recla IGAS : Problème de qualité des soins paramédicaux",
	AVG(NULLTOZERO(rs."Recherche d?établissement ou d?un professionnel")) AS "Recla IGAS : Recherche d’établissement ou d’un professionnel",
	AVG(NULLTOZERO(rs."Mise en cause attitude des professionnels")) AS "Recla IGAS : Mise en cause attitude des professionnels",
	AVG(NULLTOZERO(rs."Informations et droits des usagers")) AS "Recla IGAS : Informations et droits des usagers",
	AVG(NULLTOZERO(rs."Facturation et honoraires")) AS "Recla IGAS : Facturation et honoraires",
	AVG(NULLTOZERO(rs."Santé-environnementale")) AS "Recla IGAS : Santé environnementale",
	AVG(NULLTOZERO(rs."Activités d?esthétique réglementées")) AS "Recla IGAS : Activités d’esthétique réglementées",
	AVG(NULLTOZERO(rs.NB_EIGS)) AS  "Nombre d'EIG sur la période {param_N_3}-{param_N}",
	AVG(NULLTOZERO(rs.NB_EIAS)) AS "Nombre d'EIAS sur la période {param_N_3}-{param_N}",
	AVG(NULLTOZERO(rs."Nombre d'EI sur la période 36mois") + NULLTOZERO(rs.NB_EIGS) + NULLTOZERO(rs.NB_EIAS)) AS "Somme EI + EIGS + EIAS sur la période {param_N_3}-{param_N_1}",
	AVG(NULLTOZERO(i.'ICE {param_N_1} (réalisé)')) AS 'ICE {param_N} (réalisé)',
	AVG(NULLTOZERO(i.'Inspection SUR SITE {param_N_1} - Déjà réalisée')) AS 'Inspection SUR SITE {param_N} - Déjà réalisée',
	AVG(NULLTOZERO(i.'Controle SUR PIECE {param_N_1} - Déjà réalisé')) AS 'Controle SUR PIECE {param_N} - Déjà réalisé',
	AVG(NULLTOZERO(i.'Inspection / contrôle Programmé {param_N}')) AS 'Inspection / contrôle Programmé {param_N}',
    MAX(CAST(SUBSTR(hsm."Date réelle Visite", 7, 4) as INTEGER)) as "Année dernière inspection (sur pl ou sur pi)"
    FROM tfiness_clean tf 
LEFT JOIN finess_pivoted fp on fp.finess= tf.finess
LEFT JOIN [errd_déficit_excédent_{param_N_4}] ede on tf.finess= SUBSTRING(ede."Structure - FINESS - RAISON SOCIALE", 1, 9)
LEFT JOIN [errd_déficit_excédent_{param_N_3}] ede2 on  tf.finess = SUBSTRING(ede2."Structure - FINESS - RAISON SOCIALE", 1, 9)
LEFT JOIN [errd_déficit_excédent_{param_N_2}] ede3 on tf.finess = SUBSTRING(ede3."Structure - FINESS - RAISON SOCIALE", 1, 9)
LEFT JOIN communes c on c.com = tf.com_code
LEFT JOIN departement_{param_N} d on d.dep = c.dep
LEFT JOIN region_{param_N} r on d.reg = r.reg
LEFT JOIN capacites_ehpad ce on ce."ET-N°FINESS" = tf.finess
LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
LEFT JOIN occupation_{param_N_5}_{param_N_4}  o1 on o1.finess_19 = tf.finess
LEFT JOIN occupation_{param_N_3}  o2  on o2.finess = tf.finess
LEFT JOIN clean_occupation_N_2  co3  on co3.finess = tf.finess
LEFT JOIN clean_tdb_n_2  etra on etra.finess = tf.finess
LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
LEFT JOIN EHPAD_Indicateurs_{param_N_2}_REG_agg eira on eira."et finess" = tf.finess
LEFT JOIN clean_tdb_n_4  d2 on d2.finess = tf.finess
LEFT JOIN clean_tdb_n_3  etra2 on etra2.finess = tf.finess
LEFT JOIN recla_signalement rs on rs.finess = tf.finess
LEFT JOIN inspections i on i.finess = tf.finess
LEFT JOIN HELIOS_SICEA_MISSIONS_{param_N} hsm on IIF(LENGTH("Code FINESS") = 8, '0' || "Code FINESS", "Code FINESS") = tf.finess"""

