sql_query="""
   CREATE TABLE finess_pivoted AS
  SELECT 
    "numéro finess et" as finess,
    "code d'activité" as "code_activite",
    MAX(CASE WHEN "libellé du code d'activité" = "Type d'activité indifferencié" THEN "capacité autorisée totale" ELSE NULL END) AS "Type d'activité indifferencié",
    MAX(CASE WHEN "libellé du code d'activité" = "Protection Juridique" THEN "capacité autorisée totale" ELSE NULL END) AS "Protection Juridique",
    MAX(CASE WHEN "libellé du code d'activité" = "Hébergement Complet Internat" THEN "capacité autorisée totale" ELSE NULL END) AS "Hébergement Complet Internat",
    MAX(CASE WHEN "libellé du code d'activité" = "Placement Famille d'Accueil" THEN "capacité autorisée totale" ELSE NULL END) AS "Placement Famille d'Accueil",
    MAX(CASE WHEN "libellé du code d'activité" = "Accueil temporaire avec hébergement" THEN "capacité autorisée totale" ELSE NULL END) AS "Accueil temporaire avec hébergement",
    MAX(CASE WHEN "libellé du code d'activité" = "Prestation en milieu ordinaire" THEN "capacité autorisée totale" ELSE NULL END) AS "Prestation en milieu ordinaire",
    MAX(CASE WHEN "libellé du code d'activité" = "Accueil de jour et accompagnement en milieu ordinaire" THEN "capacité autorisée totale" ELSE NULL END) AS "Accueil de jour et accompagnement en milieu ordinaire",
    MAX(CASE WHEN "libellé du code d'activité" = "Accueil de Jour" THEN "capacité autorisée totale" ELSE NULL END) AS "Accueil de Jour",
    MAX(CASE WHEN "libellé du code d'activité" = "Accueil temporaire (avec et sans hébergement)" THEN "capacité autorisée totale" ELSE NULL END) AS "Accueil temporaire (avec et sans hébergement)",
    MAX(CASE WHEN "libellé du code d'activité" = "Semi-Internat" THEN "capacité autorisée totale" ELSE NULL END) AS "Semi-Internat",
    MAX(CASE WHEN "libellé du code d'activité" = "Hébergement de Nuit Eclaté" THEN "capacité autorisée totale" ELSE NULL END) AS "Hébergement de Nuit Eclaté",
    MAX(CASE WHEN "libellé du code d'activité" = "Internat de Semaine" THEN "capacité autorisée totale" ELSE NULL END) AS "Internat de Semaine",
    MAX(CASE WHEN "libellé du code d'activité" = "Externat" THEN "capacité autorisée totale" ELSE NULL END) AS "Externat",
    MAX(CASE WHEN "libellé du code d'activité" = "Tous modes d'accueil et d'accompagnement" THEN "capacité autorisée totale" ELSE NULL END) AS "Tous modes d'accueil et d'accompagnement",
    MAX(CASE WHEN "libellé du code d'activité" = "Tous modes d'accueil avec hébergement" THEN "capacité autorisée totale" ELSE NULL END) AS "Tous modes d'accueil avec hébergement",
    MAX(CASE WHEN "libellé du code d'activité" = "Accueil de Nuit" THEN "capacité autorisée totale" ELSE NULL END) AS "Accueil de Nuit",
    MAX(CASE WHEN "libellé du code d'activité" = "Permanence téléphonique" THEN "capacité autorisée totale" ELSE NULL END) AS "Permanence téléphonique",
    MAX(CASE WHEN "libellé du code d'activité" = "Accueil temporaire de jour" THEN "capacité autorisée totale" ELSE NULL END) AS "Accueil temporaire de jour",
    MAX(CASE WHEN "libellé du code d'activité" = "Consultation Soins Externes" THEN "capacité autorisée totale" ELSE NULL END) AS "Consultation Soins Externes",
    MAX(CASE WHEN "libellé du code d'activité" = "Accueil modulable/séquentiel" THEN "capacité autorisée totale" ELSE NULL END) AS "Accueil modulable/séquentiel",
    MAX(CASE WHEN "libellé du code d'activité" = "Traitement et Cure Ambulatoire" THEN "capacité autorisée totale" ELSE NULL END) AS "Traitement et Cure Ambulatoire",
    MAX(CASE WHEN "libellé du code d'activité" = "Tous modes d'accueil (avec et sans hébergement)" THEN "capacité autorisée totale" ELSE NULL END) AS "Tous modes d'accueil (avec et sans hébergement)",
    MAX(CASE WHEN "libellé du code d'activité" = "Equipe mobile de rue" THEN "capacité autorisée totale" ELSE NULL END) AS "Equipe mobile de rue",
    MAX(CASE WHEN "libellé du code d'activité" = "Accueil et prise en charge en appartement thérapeutique" THEN "capacité autorisée totale" ELSE NULL END) AS "Accueil et prise en charge en appartement thérapeutique",
    MAX(CASE WHEN "libellé du code d'activité" = "Hospitalisation Complète" THEN "capacité autorisée totale" ELSE NULL END) AS "Hospitalisation Complète",
    MAX(CASE WHEN "libellé du code d'activité" = "Regroupement Calcules (Annexes XXIV)" THEN "capacité autorisée totale" ELSE NULL END) AS "Regroupement Calcules (Annexes XXIV)",
    MAX(CASE WHEN "libellé du code d'activité" = "Aide Judiciaire à la Gestion du Budget Familial" THEN "capacité autorisée totale" ELSE NULL END) AS "Aide Judiciaire à la Gestion du Budget Familial",
    MAX(CASE WHEN "libellé du code d'activité" = "Accompagnement Social Personnalisé" THEN "capacité autorisée totale" ELSE NULL END) AS "Accompagnement Social Personnalisé",
    MAX(CASE WHEN "libellé du code d'activité" = "Information des Tuteurs Familiaux" THEN "capacité autorisée totale" ELSE NULL END) AS "Information des Tuteurs Familiaux",
    MAX(CASE WHEN "libellé du code d'activité" = "Administration" THEN "capacité autorisée totale" ELSE NULL END) AS "Administration"
    FROM 
    finess
    GROUP BY 
    "numéro finess et", 
    "code d'activité" """
    
    