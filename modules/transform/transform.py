import sqlite3
import json
from os import listdir
import pandas as pd
from modules.init_db.init_db import conn_db
from utils import utils
from datetime import datetime
from modules.export.export import outputName
from sqlite3 import Error


def drop_existing_views(cursor, views):
    for view in views:
        try:
            cursor.execute(f"DROP VIEW IF EXISTS {view}")
            print(f"La vue {view} a été supprimée.")
        except sqlite3.OperationalError as e:
            if "no such view" in str(e):
                # Si la vue n'existe pas, ignorer cette erreur
                print(f"Aucune vue de ce type : {view}")
            else:
                # Si c'est une autre erreur, essayer de la supprimer comme une table
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {view}")
                    print(f"La table {view} a été supprimée.")
                except sqlite3.OperationalError as e:
                    print(f"Échec de la suppression de {view} : {e}")
                    
   
# Initialisation de la base de données et chargement des paramètres
def init_table(conn):
    dbname = utils.read_settings("settings/settings.json", "db", "name")
    conn = conn_db(dbname)
    cursor = conn.cursor()

    with open('settings/settings.json') as f:
        # Charger les données JSON depuis le fichier
        data = json.load(f)
    
    # Debugging: Afficher le contenu de data
    print("Contenu de data dans inittable:", data)
    
    # Vérifier si la clé 'parametres' existe dans le JSON
    if 'parametres' in data:
        param_N = data['parametres'][0]['param_N']
        param_N_1 = data['parametres'][0]['param_N_1']
        param_N_2 = data['parametres'][0]['param_N_2']
        param_N_3 = data['parametres'][0]['param_N_3']
        param_N_4 = data['parametres'][0]['param_N_4']
        param_N_5 = data['parametres'][0]['param_N_5']
        param_fin_mois = data['parametres'][0]['param_fin_mois']
        param_debut_mois = data['parametres'][0]['param_debut_mois']
        param_debut_mois_N_3 = data['parametres'][0]['param_debut_mois_N_3']
    else:
        raise KeyError("La clé 'parametres' n'existe pas dans le fichier JSON")

    # Listes des vues à supprimer
    views = [
        "tfiness_clean", "finess_pivoted", "table_recla", "igas", "table_signalement","recla_signalement_HDF", "sign","sign_HDF",
        "recla_signalement", "clean_occupation_N_2", "clean_capacite_totale_auto", 
        "clean_hebergement", "clean_tdb_n_4", "clean_tdb_n_3", "clean_tdb_n_2",
        "correspondance", "grouped_errd_charges", "grouped_errd_produitstarif",
        "grouped_errd_produits70", "grouped_errd_produitsencaiss", 
        "grouped_caph_charges", "grouped_caph_produitstarif", 
        "grouped_caph_produits70", "grouped_caph_produitsencaiss", 
        "grouped_capa_charges", "grouped_capa_produitstarif", "charges_produits",
        "inspections", "communes",]
  # Supprimer les vues existantes
    drop_existing_views(cursor, views)
    
    tfiness_clean = f"""
    CREATE TABLE tfiness_clean AS 
    SELECT IIF(LENGTH(tf_with.finess) = 8, '0' || tf_with.finess, tf_with.finess) as finess,
           tf_with.categ_lib, tf_with.categ_code, tf_with.rs,
           IIF(LENGTH(tf_with.ej_finess) = 8, '0' || tf_with.ej_finess, tf_with.ej_finess) as ej_finess,
           tf_with.ej_rs, tf_with.statut_jur_lib,
           IIF(tf_with.adresse_num_voie IS NULL, '', SUBSTRING(CAST(tf_with.adresse_num_voie as TEXT), 1, LENGTH(CAST(tf_with.adresse_num_voie as TEXT)) - 2) || ' ') ||
           IIF(tf_with.adresse_comp_voie IS NULL, '', tf_with.adresse_comp_voie || ' ') ||
           IIF(tf_with.adresse_type_voie IS NULL, '', tf_with.adresse_type_voie || ' ') ||
           IIF(tf_with.adresse_nom_voie IS NULL, '', tf_with.adresse_nom_voie || ' ') ||
           IIF(tf_with.adresse_lieuditbp IS NULL, '', tf_with.adresse_lieuditbp || ' ') ||
           IIF(tf_with.adresse_lib_routage IS NULL, '', tf_with.adresse_lib_routage) as adresse,
           IIF(LENGTH(tf_with.ej_finess) = 8, '0' || tf_with.ej_finess, tf_with.ej_finess) as ej_finess,
           CAST(adresse_code_postal AS INTEGER) as adresse_code_postal,
           tf_with.com_code 
    FROM 't-finess' tf_with 
    WHERE tf_with.categ_code IN (238, 190, 189,395,246,249,437,253, 382, 252,192, 196, 183, 195,194, 186, 402, 255, 445,182, 446,464)
    """
    cursor.execute(tfiness_clean)
    conn.commit()
    print("tfiness_clean a été ajouté")
    
    finess_pivoted = f"""
    CREATE TABLE finess_pivoted AS
  SELECT 
    "Numéro FINESS ET" as finess,
    "Code d'activité" as "code_activite",
    MAX(CASE WHEN "Libellé du code d'activité" = "Type d'activité indifferencié" THEN "Capacité autorisée totale" ELSE NULL END) AS "Type d'activité indifferencié",
    MAX(CASE WHEN "Libellé du code d'activité" = "Protection Juridique" THEN "Capacité autorisée totale" ELSE NULL END) AS "Protection Juridique",
    MAX(CASE WHEN "Libellé du code d'activité" = "Hébergement Complet Internat" THEN "Capacité autorisée totale" ELSE NULL END) AS "Hébergement Complet Internat",
    MAX(CASE WHEN "Libellé du code d'activité" = "Placement Famille d'Accueil" THEN "Capacité autorisée totale" ELSE NULL END) AS "Placement Famille d'Accueil",
    MAX(CASE WHEN "Libellé du code d'activité" = "Accueil temporaire avec hébergement" THEN "Capacité autorisée totale" ELSE NULL END) AS "Accueil temporaire avec hébergement",
    MAX(CASE WHEN "Libellé du code d'activité" = "Prestation en milieu ordinaire" THEN "Capacité autorisée totale" ELSE NULL END) AS "Prestation en milieu ordinaire",
    MAX(CASE WHEN "Libellé du code d'activité" = "Accueil de jour et accompagnement en milieu ordinaire" THEN "Capacité autorisée totale" ELSE NULL END) AS "Accueil de jour et accompagnement en milieu ordinaire",
    MAX(CASE WHEN "Libellé du code d'activité" = "Accueil de Jour" THEN "Capacité autorisée totale" ELSE NULL END) AS "Accueil de Jour",
    MAX(CASE WHEN "Libellé du code d'activité" = "Accueil temporaire (avec et sans hébergement)" THEN "Capacité autorisée totale" ELSE NULL END) AS "Accueil temporaire (avec et sans hébergement)",
    MAX(CASE WHEN "Libellé du code d'activité" = "Semi-Internat" THEN "Capacité autorisée totale" ELSE NULL END) AS "Semi-Internat",
    MAX(CASE WHEN "Libellé du code d'activité" = "Hébergement de Nuit Eclaté" THEN "Capacité autorisée totale" ELSE NULL END) AS "Hébergement de Nuit Eclaté",
    MAX(CASE WHEN "Libellé du code d'activité" = "Internat de Semaine" THEN "Capacité autorisée totale" ELSE NULL END) AS "Internat de Semaine",
    MAX(CASE WHEN "Libellé du code d'activité" = "Externat" THEN "Capacité autorisée totale" ELSE NULL END) AS "Externat",
    MAX(CASE WHEN "Libellé du code d'activité" = "Tous modes d'accueil et d'accompagnement" THEN "Capacité autorisée totale" ELSE NULL END) AS "Tous modes d'accueil et d'accompagnement",
    MAX(CASE WHEN "Libellé du code d'activité" = "Tous modes d'accueil avec hébergement" THEN "Capacité autorisée totale" ELSE NULL END) AS "Tous modes d'accueil avec hébergement",
    MAX(CASE WHEN "Libellé du code d'activité" = "Accueil de Nuit" THEN "Capacité autorisée totale" ELSE NULL END) AS "Accueil de Nuit",
    MAX(CASE WHEN "Libellé du code d'activité" = "Permanence téléphonique" THEN "Capacité autorisée totale" ELSE NULL END) AS "Permanence téléphonique",
    MAX(CASE WHEN "Libellé du code d'activité" = "Accueil temporaire de jour" THEN "Capacité autorisée totale" ELSE NULL END) AS "Accueil temporaire de jour",
    MAX(CASE WHEN "Libellé du code d'activité" = "Consultation Soins Externes" THEN "Capacité autorisée totale" ELSE NULL END) AS "Consultation Soins Externes",
    MAX(CASE WHEN "Libellé du code d'activité" = "Accueil modulable/séquentiel" THEN "Capacité autorisée totale" ELSE NULL END) AS "Accueil modulable/séquentiel",
    MAX(CASE WHEN "Libellé du code d'activité" = "Traitement et Cure Ambulatoire" THEN "Capacité autorisée totale" ELSE NULL END) AS "Traitement et Cure Ambulatoire",
    MAX(CASE WHEN "Libellé du code d'activité" = "Tous modes d'accueil (avec et sans hébergement)" THEN "Capacité autorisée totale" ELSE NULL END) AS "Tous modes d'accueil (avec et sans hébergement)",
    MAX(CASE WHEN "Libellé du code d'activité" = "Equipe mobile de rue" THEN "Capacité autorisée totale" ELSE NULL END) AS "Equipe mobile de rue",
    MAX(CASE WHEN "Libellé du code d'activité" = "Accueil et prise en charge en appartement thérapeutique" THEN "Capacité autorisée totale" ELSE NULL END) AS "Accueil et prise en charge en appartement thérapeutique",
    MAX(CASE WHEN "Libellé du code d'activité" = "Hospitalisation Complète" THEN "Capacité autorisée totale" ELSE NULL END) AS "Hospitalisation Complète",
    MAX(CASE WHEN "Libellé du code d'activité" = "Regroupement Calcules (Annexes XXIV)" THEN "Capacité autorisée totale" ELSE NULL END) AS "Regroupement Calcules (Annexes XXIV)",
    MAX(CASE WHEN "Libellé du code d'activité" = "Aide Judiciaire à la Gestion du Budget Familial" THEN "Capacité autorisée totale" ELSE NULL END) AS "Aide Judiciaire à la Gestion du Budget Familial",
    MAX(CASE WHEN "Libellé du code d'activité" = "Accompagnement Social Personnalisé" THEN "Capacité autorisée totale" ELSE NULL END) AS "Accompagnement Social Personnalisé",
    MAX(CASE WHEN "Libellé du code d'activité" = "Information des Tuteurs Familiaux" THEN "Capacité autorisée totale" ELSE NULL END) AS "Information des Tuteurs Familiaux",
    MAX(CASE WHEN "Libellé du code d'activité" = "Administration" THEN "Capacité autorisée totale" ELSE NULL END) AS "Administration"
    FROM 
    finess
    GROUP BY 
     "Numéro FINESS ET", 
    "Code d'activité" """
    cursor.execute(finess_pivoted)
    conn.commit()
    print("finess_pivoted a été ajouté")


    table_recla = f"""
    CREATE TABLE table_recla AS 
    SELECT IIF(LENGTH(se."N° FINESS/RPPS") = 8, '0'|| se."N° FINESS/RPPS", se."N° FINESS/RPPS") as finess, 
           COUNT(*) as nb_recla 
    FROM reclamations_mars20_mars"""+param_N_1+""" se 
    WHERE se."N° FINESS/RPPS" IS NOT NULL AND (se.Signalement = 'Non' or se.Signalement IS NULL) 
    GROUP BY 1
    """
    cursor.execute(table_recla)
    conn.commit()
    print("table_recla a été ajouté")

    igas = f"""
    CREATE TABLE igas AS 
    SELECT 
	IIF(LENGTH(se."N° FINESS/RPPS" )= 8, '0'|| se."N° FINESS/RPPS", se."N° FINESS/RPPS") as finess, 
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
    FROM reclamations_mars20_mars2023 se
    WHERE 
	(se.Signalement = 'Non' or se.Signalement IS NULL)
	AND se."N° FINESS/RPPS"  IS NOT NULL
    GROUP BY 1
    """
    cursor.execute(igas)
    conn.commit()
    print("igas a été ajouté")

    table_signalement = f"""
    CREATE TABLE table_signalement AS 
   SELECT "Déclarant organisme
N° FINESS" , 
"Survenue du cas en collectivité
N° FINESS" ,
           "Date de réception", 
           "Réclamation", 
           "Déclarant 
Type Etablissement (Si ES/EMS)" , 
           "Ceci est un EIGS", 
           "Famille principale", 
           "Nature principale"  
    FROM all_sivss
    """
    cursor.execute(table_signalement)
    conn.commit()
    print("table_signalement a été ajouté")

    sign = f"""
    CREATE TABLE sign AS 
    SELECT 
	finess,
	COUNT(*) as nb_signa,
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non',1,0)) as "Nombre d'EI sur la période 36mois",
	SUM(IIF("Ceci est un EIGS" = 'Oui', 1, 0)) as NB_EIGS,
	SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non',1,0)) AS NB_EIAS,
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non',1,0)) AS "Somme EI + EIGS + EIAS sur la période",
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Acte de prévention',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Acte de prévention', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Acte de prévention',1,0)) AS 'nb EI/EIG : Acte de prévention',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Autre prise en charge',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Autre prise en charge', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Autre prise en charge',1,0)) AS 'nb EI/EIG : Autre prise en charge',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Chute',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Chute', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Chute',1,0)) AS 'nb EI/EIG : Chute',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)',1,0)) AS 'nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Dispositif médical',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Dispositif médical', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Dispositif médical',1,0)) AS 'nb EI/EIG : Dispositif médical',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Fausse route',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Fausse route', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Fausse route',1,0)) AS 'nb EI/EIG : Fausse route',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Infection associée aux soins (IAS) hors ES',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Infection associée aux soins (IAS) hors ES', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Infection associée aux soins (IAS) hors ES',1,0)) AS 'nb EI/EIG : Infection associée aux soins (IAS) hors ES',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Infection associée aux soins en EMS et ambulatoire (IAS hors ES)',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Infection associée aux soins en EMS et ambulatoire (IAS hors ES)', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Infection associée aux soins en EMS et ambulatoire (IAS hors ES)',1,0)) AS 'nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Parcours/Coopération interprofessionnelle',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Parcours/Coopération interprofessionnelle', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Parcours/Coopération interprofessionnelle',1,0)) AS 'nb EI/EIG : Parcours/Coopération interprofessionnelle',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge chirurgicale',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Prise en charge chirurgicale', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge chirurgicale',1,0)) AS 'nb EI/EIG : Prise en charge chirurgicale',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge diagnostique',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Prise en charge diagnostique', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge diagnostique',1,0)) AS 'nb EI/EIG : Prise en charge diagnostique',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge en urgence',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Prise en charge en urgence', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge en urgence',1,0)) AS 'nb EI/EIG : Prise en charge en urgence',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge médicamenteuse',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Prise en charge médicamenteuse', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge médicamenteuse',1,0)) AS 'nb EI/EIG : Prise en charge médicamenteuse',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge des cancers',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Prise en charge des cancers', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge des cancers',1,0)) AS 'nb EI/EIG : Prise en charge des cancers',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge psychiatrique',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Prise en charge psychiatrique', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Prise en charge psychiatrique',1,0)) AS 'nb EI/EIG : Prise en charge psychiatrique',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Suicide',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Suicide', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Suicide',1,0)) AS 'nb EI/EIG : Suicide',
	SUM(IIF("Famille principale" = 'Evénements/incidents dans un établissement ou organisme' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Tentative de suicide',1,0)) + SUM(IIF("Ceci est un EIGS" = 'Oui' AND "Nature principale" = 'Tentative de suicide', 1, 0)) + SUM(IIF("Famille principale" = 'Evénements indésirables/graves associés aux soins' AND "Ceci est un EIGS" = 'Non' AND "Nature principale" = 'Tentative de suicide',1,0)) AS 'nb EI/EIG : Tentative de suicide'
    FROM
     (SELECT 
		CASE 
			WHEN substring(tb. "Déclarant organisme
N° FINESS" ,-9) == substring(CAST(tb."Survenue du cas en collectivité
N° FINESS"  as text),1,9)
				THEN substring(tb. "Déclarant organisme
N° FINESS" ,-9)
			WHEN tb."Survenue du cas en collectivité
N° FINESS"  IS NULL
				THEN substring(tb. "Déclarant organisme
N° FINESS" ,-9)
			ELSE 
				substring(CAST(tb."Survenue du cas en collectivité
N° FINESS"  as text),1,9)
		END as finess, *
	 FROM table_signalement tb
	 WHERE
	 tb."Réclamation" != 'Oui') as sub_table
    GROUP BY 1"""
    cursor.execute(sign)
    conn.commit()
    print("sign a été ajouté")
    sign_HDF="""CREATE TABLE sign_HDF AS 
		SELECT IIF(LENGTH(se."N° FINESS/RPPS" )= 8, '0'|| se."N° FINESS/RPPS", se."N° FINESS/RPPS") as finess,
		COUNT(*) as nb_signa 
		FROM RECLAMATIONs_mars20_mars2023 se 
		WHERE se.signalement = 'Oui'AND se."N° FINESS/RPPS"  IS NOT NULL 
		GROUP BY 1"""
    cursor.execute(sign_HDF)
    conn.commit()
    print("sign_HDF a été ajouté")

    recla_signalement = f"""
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
FROM 
	tfiness_clean tfc 
	LEFT JOIN table_recla tr on tr.finess = tfc.finess
	LEFT JOIN igas i on i.finess = tfc.finess
	LEFT JOIN sign s on s.finess = tfc.finess
    """
    cursor.execute(recla_signalement)
    conn.commit()
    print("recla_signalement a été ajouté")
    
    recla_signalement_HDF= """CREATE TABLE recla_signalement_HDF AS
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
	LEFT JOIN table_recla tr on tr.finess = tfc.finess
	LEFT JOIN igas i on i.finess = tfc.finess
	LEFT JOIN sign_HDF sh on sh.finess = tfc.finess"""
 
    cursor.execute(recla_signalement_HDF)
    conn.commit()
    print("recla_signalement_HDF a été ajouté")
    
    clean_occupation_N_2 = f"""
    CREATE TABLE clean_occupation_N_2 AS 
    SELECT IIF(LENGTH(o3.finess) = 8, '0'|| o3.finess, o3.finess) as finess, 
           o3.taux_occ_"""+param_N_2+""",
           o3.nb_lits_autorises_installes,
           o3.nb_lits_occ_"""+param_N_2+""",
           o3.taux_occ_trimestre3 
    FROM occupation_"""+param_N_2+""" o3
    """
    cursor.execute(clean_occupation_N_2)
    conn.commit()
    print("clean_occupation_N_2 a été ajouté")

    clean_capacite_totale_auto = f"""
    CREATE TABLE clean_capacite_totale_auto AS 
	SELECT IIF(LENGTH(cta."Étiquettes de lignes") = 8, '0'|| cta."Étiquettes de lignes", cta."Étiquettes de lignes") as finess, 
           cta."Somme de Capacité autorisée totale " as somme_de_capacite_autorisee_totale_
    FROM capacite_totale_auto cta
    """
    cursor.execute(clean_capacite_totale_auto)
    conn.commit()
    print("clean_capacite_totale_auto a été ajouté")

    clean_hebergement = f"""
    CREATE TABLE clean_hebergement AS 
    SELECT IIF(LENGTH(h.finesset) = 8, '0'|| h.finesset, h.finesset) as finess,
           h."prixHebPermCs" 
    FROM hebergement h
    """
    cursor.execute(clean_hebergement)
    conn.commit()
    print("clean_hebergement a été ajouté")

    clean_tdb_n_4 = f"""
    CREATE TABLE clean_tdb_n_4 AS 
    SELECT IIF(LENGTH(tdb_"""+param_N_4+"""."finess géographique") = 8, '0'|| tdb_"""+param_N_4+"""."finess géographique", tdb_"""+param_N_4+"""."finess géographique") as finess, *
    FROM "export-tdbesms-"""+param_N_4+"""-region_agg" tdb_"""+param_N_4+"""
    """
    cursor.execute(clean_tdb_n_4)
    conn.commit()
    print("clean_tdb_n_4 a été ajouté")

    clean_tdb_n_3 = f"""
    CREATE TABLE clean_tdb_n_3 AS 
    SELECT IIF(LENGTH(tdb_"""+param_N_3+"""."finess géographique") = 8, '0'|| tdb_"""+param_N_3+"""."finess géographique", tdb_"""+param_N_3+"""."finess géographique") as finess, *
    FROM "export-tdbesms-"""+param_N_3+"""-region-agg" tdb_"""+param_N_3+"""
    """
    cursor.execute(clean_tdb_n_3)
    conn.commit()
    print("clean_tdb_n_3 a été ajouté")

    clean_tdb_n_2 = f"""
    CREATE TABLE clean_tdb_n_2 AS 
    SELECT IIF(LENGTH(tdb_"""+param_N_2+"""."finess géographique") = 8, '0'|| tdb_"""+param_N_2+"""."finess géographique", tdb_"""+param_N_2+"""."finess géographique") as finess, *
    FROM "export-tdbesms-"""+param_N_2+"""-region-agg" tdb_"""+param_N_2+"""
    """
    cursor.execute(clean_tdb_n_2)
    conn.commit()
    print("clean_tdb_n_2 a été ajouté")


    correspondance =f"""
    CREATE TABLE correspondance AS 
    SELECT SUBSTRING(cecpp."FINESS - RS ET", 1, 9) as finess,
           cecpp.CADRE 
    FROM choix_errd_ca_pa_ph cecpp 
    LEFT JOIN doublons_errd_ca dou on SUBSTRING(dou.finess, 1, 9) = SUBSTRING(cecpp."FINESS - RS ET", 1, 9) AND cecpp.cadre != 'ERRD' 
    WHERE dou.finess IS NULL
    """
    cursor.execute(correspondance)
    conn.commit()
    print("correspondance a été ajouté")

    grouped_errd_charges = f"""
    CREATE TABLE grouped_errd_charges AS 
    SELECT SUBSTRING(ec."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess,
           SUM(ec."Charges d'exploitation") as sum_charges_dexploitation 
    FROM errd_charges ec 
    GROUP BY 1
    """
    cursor.execute(grouped_errd_charges)
    conn.commit()
    print("grouped_errd_charges a été ajouté")

    grouped_errd_produitstarif = f"""
    CREATE TABLE grouped_errd_produitstarif AS 
    SELECT SUBSTRING(ep."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess,
           SUM(ep."GROUPE I : PRODUITS DE LA TARIFICATION") as sum_groupe_i__produits_de_la_tarification 
    FROM errd_produitstarif ep 
    GROUP BY 1
    """
    cursor.execute(grouped_errd_produitstarif)
    conn.commit()
    print("grouped_errd_produitstarif a été ajouté")

    grouped_errd_produits70 = f"""
    CREATE TABLE grouped_errd_produits70 AS 
    SELECT SUBSTRING(ep2."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
           SUM(ep2.unnamed_1) as sum_produits70 
    FROM errd_produits70 ep2 
    GROUP BY 1
    """
    cursor.execute(grouped_errd_produits70)
    conn.commit()
    print("grouped_errd_produits70 a été ajouté")

    grouped_errd_produitsencaiss = f"""
    CREATE TABLE grouped_errd_produitsencaiss AS 
    SELECT SUBSTRING(ep3."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
           SUM(ep3."Produits d'exploitation") as sum_produits_dexploitation 
    FROM errd_produitsencaiss ep3 
    GROUP BY 1
    """
    cursor.execute(grouped_errd_produitsencaiss)
    conn.commit()
    print("grouped_errd_produitsencaiss a été ajouté")

    grouped_caph_charges = f"""
    CREATE TABLE grouped_caph_charges AS 
    SELECT SUBSTRING(cch."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess,
           SUM(cch."Charges d'exploitation") as sum_charges_dexploitation 
    FROM caph_charges cch  
    GROUP BY 1
    """
    cursor.execute(grouped_caph_charges)
    conn.commit()
    print("grouped_caph_charges a été ajouté")

    grouped_caph_produitstarif = f"""
    CREATE TABLE grouped_caph_produitstarif AS 
    SELECT SUBSTRING(cch2."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess,
           SUM(cch2."GROUPE I : PRODUITS DE LA TARIFICATION") as sum_groupe_i__produits_de_la_tarification 
    FROM caph_produitstarif cch2 
    GROUP BY 1
    """
    cursor.execute(grouped_caph_produitstarif)
    conn.commit()
    print("grouped_caph_produitstarif a été ajouté")

    grouped_caph_produits70 = f"""
    CREATE TABLE grouped_caph_produits70 AS 
    SELECT SUBSTRING(cch3."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
           SUM(cch3.unnamed_1) as sum_produits70 
    FROM caph_produits70 cch3 
    GROUP BY 1
    """
    cursor.execute(grouped_caph_produits70)
    conn.commit()
    print("grouped_caph_produits70 a été ajouté")

    grouped_caph_produitsencaiss = f"""
    CREATE TABLE grouped_caph_produitsencaiss AS 
    SELECT SUBSTRING(cch4."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
           SUM(cch4."Produits d'exploitation") as sum_produits_dexploitation 
    FROM caph_produitsencaiss cch4 
    GROUP BY 1
    """
    cursor.execute(grouped_caph_produitsencaiss)
    conn.commit()
    print("grouped_caph_produitsencaiss a été ajouté")

    grouped_capa_charges = f"""
    CREATE TABLE grouped_capa_charges AS 
    SELECT SUBSTRING(cc."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
           SUM(cc."CHARGES D'EXPLOITATION") as sum_charges_dexploitation 
    FROM capa_charges cc  
    GROUP BY 1
    """
    cursor.execute(grouped_capa_charges)
    conn.commit()
    print("grouped_capa_charges a été ajouté")

    grouped_capa_produitstarif = f"""
    CREATE TABLE grouped_capa_produitstarif AS 
    SELECT SUBSTRING(cpt."Structure - FINESS - RAISON SOCIALE", 1, 9) as finess, 
           SUM(cpt."PRODUITS DE L'EXERCICE") as sum_groupe_i__produits_de_la_tarification                 
    FROM capa_produitstarif cpt  
    GROUP BY 1
    """
    cursor.execute(grouped_capa_produitstarif)
    conn.commit()
    print("grouped_capa_produitstarif a été ajouté")

    charges_produits = f"""
    CREATE TABLE charges_produits AS 
    SELECT cor.finess, 
           CASE WHEN cor.CADRE = 'ERRD' THEN gec.sum_charges_dexploitation 
                WHEN cor.CADRE = 'CA PA' THEN gc.sum_charges_dexploitation 
                WHEN cor.CADRE = 'CA PH' THEN gcch.sum_charges_dexploitation 
           END as 'Total des charges', 
           CASE WHEN cor.CADRE = 'ERRD' THEN gep.sum_groupe_i__produits_de_la_tarification 
                WHEN cor.CADRE = 'CA PA' THEN gcp.sum_groupe_i__produits_de_la_tarification 
                WHEN cor.CADRE = 'CA PH' THEN gcch2.sum_groupe_i__produits_de_la_tarification 
           END as 'Produits de la tarification', 
           CASE WHEN cor.CADRE = 'ERRD' THEN gep2.sum_produits70 
                WHEN cor.CADRE = 'CA PA' THEN 0 
                WHEN cor.CADRE = 'CA PH' THEN gcch3.sum_produits70 
           END as 'Produits du compte 70', 
           CASE WHEN cor.CADRE = 'ERRD' THEN gep3.sum_produits_dexploitation 
                WHEN cor.CADRE = 'CA PA' THEN 0 
                WHEN cor.CADRE = 'CA PH' THEN gcch4.sum_produits_dexploitation 
           END as 'Total des produits (hors c/775, 777, 7781 et 78)' 
    FROM correspondance cor 
    LEFT JOIN grouped_errd_charges gec on gec.finess = cor.finess AND cor.cadre = 'ERRD'  
    LEFT JOIN grouped_errd_produitstarif gep on gep.finess = cor.finess AND cor.cadre = 'ERRD' 
    LEFT JOIN grouped_errd_produits70 gep2 on gep2.finess = cor.finess AND cor.cadre = 'ERRD' 
    LEFT JOIN grouped_errd_produitsencaiss gep3 on gep3.finess = cor.finess AND cor.cadre = 'ERRD' 
    LEFT JOIN grouped_caph_charges gcch on gcch.finess = cor.finess AND cor.cadre = 'CA PH' 
    LEFT JOIN grouped_caph_produitstarif gcch2 on gcch2.finess = cor.finess AND cor.cadre = 'CA PH' 
    LEFT JOIN grouped_caph_produits70 gcch3 on gcch3.finess = cor.finess AND cor.cadre = 'CA PH' 
    LEFT JOIN grouped_caph_produitsencaiss gcch4 on gcch4.finess = cor.finess AND cor.cadre = 'CA PH' 
    LEFT JOIN grouped_capa_charges gc on gc.finess = cor.finess AND cor.cadre = 'CA PA' 
    LEFT JOIN grouped_capa_produitstarif gcp on gcp.finess = cor.finess AND cor.cadre = 'CA PA'
    """
    cursor.execute(charges_produits)
    conn.commit()
    print("charges_produits a été ajouté")

    inspections = f"""
    CREATE TABLE inspections AS 
    SELECT finess, SUM(IIF(realise = 'oui', 1, 0)) as 'ICE """+param_N_1+""" (réalisé)', 
           SUM(IIF(realise = 'oui' AND CTRL_PL_PI = 'Contrôle sur place', 1, 0)) as 'Inspection SUR SITE """+param_N_1+"""- Déjà réalisée', 
           SUM(IIF(realise = 'oui' AND CTRL_PL_PI = 'Contrôle sur pièces', 1, 0)) as 'Controle SUR PIECE """+param_N_1+"""- Déjà réalisé', 
           SUM(IIF(programme = 'oui', 1, 0)) as 'Inspection / contrôle Programmé """+param_N+"""'
    FROM (SELECT 
            finess, 
            "Identifiant de la mission",
            [Date provisoire "Visite"],
            [Date réelle "Visite"],
            CTRL_PL_PI,
            IIF(CAST(SUBSTR([Date réelle "Visite"], 7, 4) || SUBSTR([Date réelle "Visite"], 4, 2) || SUBSTR([Date réelle "Visite"], 1, 2) AS INTEGER) <= 20231231, "oui", '') as realise,
            IIF(CAST(SUBSTR([Date réelle "Visite"], 7, 4) || SUBSTR([Date réelle "Visite"], 4, 2) || SUBSTR([Date réelle "Visite"], 1, 2) AS INTEGER) > 20231231 AND CAST(SUBSTR([Date provisoire "Visite"], 7, 4) || SUBSTR([Date provisoire "Visite"], 4, 2) || SUBSTR([Date provisoire "Visite"], 1, 2) AS INTEGER) > 20231231, "oui", '') as programme
          FROM (SELECT 
                *,
                IIF(LENGTH("Code FINESS")= 8, '0'|| "Code FINESS", "Code FINESS") as finess,
                "Modalité d'investigation" AS CTRL_PL_PI
                FROM HELIOS_SICEA_MISSIONS_"""+param_N_4+"""_20230113
                WHERE CAST(SUBSTR([Date réelle "Visite"], 7, 4) || SUBSTR([Date réelle "Visite"], 4, 2) || SUBSTR([Date réelle "Visite"], 1, 2) AS INTEGER) >= 20230101
                AND "Code FINESS" IS NOT NULL) brut 
          GROUP BY finess, "Identifiant de la mission", [Date provisoire "Visite"], [Date réelle "Visite"], CTRL_PL_PI) brut_agg 
    GROUP BY finess
    """
    cursor.execute(inspections)
    conn.commit()
    print("inspections a été ajouté")


    communes = f"""
    CREATE TABLE communes AS 
    SELECT c.com, c.dep, c.ncc   
    FROM commune_"""+param_N_2+""" c  
    WHERE c.reg IS NOT NULL UNION 
    ALL SELECT c.com, c2.dep, c.ncc 
    FROM commune_"""+param_N_2+""" c  
    LEFT JOIN commune_"""+param_N_2+""" c2 on c.comparent = c2.com AND c2.dep IS NOT NULL 
    WHERE c.reg IS NULL and c.com != c.comparent
    """
    cursor.execute(communes)
    conn.commit()
    print("communes a été ajouté")
    return


# Fonction principale pour exécuter les transformations
def execute_transform(region):
    dbname = utils.read_settings("settings/settings.json", "db", "name")
    conn = None
    try:

            conn = sqlite3.connect(dbname + '.sqlite')
            conn.create_function("NULLTOZERO",1, nullToZero)
            conn.create_function("MOY3", 3, moy3)
            cursor = conn.cursor()
            with open('settings/settings.json') as f:
                data= json.load(f)
            # Extraire les paramètres
            param_N =data["parametres"][0]["param_N"]
            param_N_1 =data["parametres"][0]["param_N_1"]
            param_N_2 = data["parametres"][0]["param_N_2"]
            param_N_3 = data["parametres"][0]["param_N_3"]
            param_N_4 = data["parametres"][0]["param_N_4"]
            param_N_5 = data["parametres"][0]["param_N_5"]
                
            if region == "32":
            # Exécution requête ciblage HDF
                print('Exécution requête ciblage HDF')
                df_ciblage = f"""
                SELECT 
                r.ncc as Region,
                d.dep as "Code dép",
                d.ncc AS "Département",
                tf.categ_lib as Catégorie,
                tf.finess as "FINESS géographique",
                tf.rs as "Raison sociale ET",
                tf.ej_finess as "FINESS juridique",
                tf.ej_rs as "Raison sociale EJ",
                tf.statut_jur_lib as "Statut juridique",
                tf.adresse as Adresse,
                IIF(LENGTH(tf.adresse_code_postal) = 4, '0'|| tf.adresse_code_postal, tf.adresse_code_postal) AS "Code postal",
                c.NCC AS "Commune",
                IIF(LENGTH(tf.com_code) = 4, '0'|| tf.com_code, tf.com_code) AS "Code commune INSEE",
                CASE
                    WHEN tf.categ_code = 500
                    THEN CAST(NULLTOZERO(ce."TOTAL Héberg. Comp. Inter. Places Autorisées") as INTEGER) + CAST(NULLTOZERO(ce."TOTAL Accueil de Jour Places Autorisées") as INTEGER) + CAST(NULLTOZERO(ce."TOTAL Accueil de nuit Places Autorisées") as INTEGER)
                    ELSE CAST(ccta.somme_de_capacite_autorisee_totale_ as INTEGER)
                END as "Capacité totale autorisée",
                fp."Type d'activité indifferencié" AS "Type d'activité indifferencié",
                fp."Protection Juridique" AS "Protection Juridique",
                fp."Hébergement Complet Internat" AS "Hébergement Complet Internat",
                fp."Placement Famille d'Accueil" AS "Placement Famille d'Accueil",
                fp."Accueil temporaire avec hébergement" AS "Accueil temporaire avec hébergement",
                fp."Prestation en milieu ordinaire" AS "Prestation en milieu ordinaire",
                fp."Accueil de jour et accompagnement en milieu ordinaire" AS "Accueil de jour et accompagnement en milieu ordinaire",
                fp."Accueil de Jour" AS "Accueil de Jour",
                fp."Accueil temporaire (avec et sans hébergement)" AS "Accueil temporaire (avec et sans hébergement)",
                fp."Semi-Internat" AS "Semi-Internat",
                fp."Hébergement de Nuit Eclaté" AS "Hébergement de Nuit Eclaté",
                fp."Internat de Semaine" AS "Internat de Semaine",
                fp."Externat" AS "Externat",
                fp."Tous modes d'accueil et d'accompagnement" AS "Tous modes d'accueil et d'accompagnement",
                fp."Tous modes d'accueil avec hébergement" AS "Tous modes d'accueil avec hébergement",
                fp."Accueil de Nuit" AS "Accueil de Nuit",
                fp."Permanence téléphonique" AS "Permanence téléphonique",
                fp."Accueil temporaire de jour" AS "Accueil temporaire de jour",
                fp."Consultation Soins Externes" AS "Consultation Soins Externes",
                fp."Accueil modulable/séquentiel" AS "Accueil modulable/séquentiel",
                fp."Traitement et Cure Ambulatoire" AS "Traitement et Cure Ambulatoire",
                fp."Tous modes d'accueil (avec et sans hébergement)" AS "Tous modes d'accueil (avec et sans hébergement)",
                fp."Equipe mobile de rue" AS "Equipe mobile de rue",
                fp."Accueil et prise en charge en appartement thérapeutique" AS "Accueil et prise en charge en appartement thérapeutique",
                fp."Hospitalisation Complète" AS "Hospitalisation Complète",
                fp."Regroupement Calcules (Annexes XXIV)" AS "Regroupement Calcules (Annexes XXIV)",
                fp."Aide Judiciaire à la Gestion du Budget Familial" AS "Aide Judiciaire à la Gestion du Budget Familial",
                fp."Accompagnement Social Personnalisé" AS "Accompagnement Social Personnalisé",
                fp."Information des Tuteurs Familiaux" AS "Information des Tuteurs Familiaux",
                fp."Administration" AS "Administration",
                co3.nb_lits_occ_"""+param_N_2+""" as "Nombre de résidents au 31/12/"""+param_N_2+"""",
                etra."Nombre total de chambres installées au 31.12" as "Nombre de places installées au 31/12/"""+param_N_2+"""",
                ROUND(CAST(REPLACE(eira."Taux_plus_10_médics_cip13", ",", ".") AS FLOAT),2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
                ROUND(CAST(REPLACE(eira.taux_atu, ",", ".") AS FLOAT),2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
                --"" as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
                CAST(REPLACE(taux_hospit_mco, ",", ".") AS FLOAT) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
                CAST(REPLACE(taux_hospit_had, ",", ".") AS FLOAT) as "Taux de recours à l'HAD des résidents d'EHPAD",
                "deficit ou excédent" AS "deficit ou excédent """+param_N_4+""" ",
                "deficit ou excédent" AS "deficit ou excédent """+param_N_3+""" ",
                "deficit ou excédent" AS "deficit ou excédent """+param_N_2+""" ",	     
                "" as "Saisie des indicateurs du TDB MS (campagne """+param_N_2+""")",
                CAST(d2."Taux d'absentéisme (hors formation) en %" as decmail) as "Taux d'absentéisme """+param_N_4+"""",
                etra2."Taux d'absentéisme (hors formation) en %"    as "Taux d'absentéisme """+param_N_4+"'"""",
                etra."Taux d'absentéisme (hors formation) en %" as "Taux d'absentéisme """+param_N_2+"""",
                ROUND(MOY3(d2."Taux d'absentéisme (hors formation) en %" ,etra2."Taux d'absentéisme (hors formation) en %", etra."Taux d'absentéisme (hors formation) en %") ,2) as "Absentéisme moyen sur la période """+param_N_4+"""-"""+param_N_2+"""",
                CAST(d2."Taux de rotation des personnels" as decimal) as "Taux de rotation du personnel titulaire """+param_N_4+"""",
                etra2."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_3+"""",
                etra."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_2+"""",
                ROUND(MOY3(d2."Taux de rotation des personnels" , etra2."Taux de rotation des personnels" , etra."Taux de rotation des personnels"), 2) as "Rotation moyenne du personnel sur la période """+param_N_4+"""-"""+param_N_2+"""",
                CAST(d2."Taux d'ETP vacants" as decimal) as "ETP vacants """+param_N_4+"""",
                etra2."Taux d'ETP vacants"  as "ETP vacants """+param_N_3+"""",
                etra."Taux d'ETP vacants" as "ETP vacants """+param_N_2+"""",
                etra."Dont taux d'ETP vacants concernant la fonction SOINS" as "dont fonctions soins """+param_N_2+"""",
                etra."Dont taux d'ETP vacants concernant la fonction SOCIO EDUCATIVE"as "dont fonctions socio-éducatives """+param_N_2+"""", 
                CAST(REPLACE(d2."Taux de prestations externes sur les prestations directes",',','.')as decimal) as "Taux de prestations externes sur les prestations directes """+param_N_4+"""",
                etra2."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes """+param_N_3+"""", 
                etra."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes """+param_N_2+"""",
                ROUND(MOY3(d2."Taux de prestations externes sur les prestations directes" , etra2."Taux de prestations externes sur les prestations directes" , etra."Taux de prestations externes sur les prestations directes") ,4) as "Taux moyen de prestations externes sur les prestations directes",
                ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_4+"""",
                ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) as "Nombre total d'ETP par usager en """+param_N_3+"""",
                ROUND((etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_2+"""",
                MOY3(ROUND(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) , ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) , 
                ROUND((etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2),2))AS "Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+"""",
                ROUND((etra."ETP Paramédical" + etra."ETP Médical")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "ETP 'soins' par usager en """+param_N_2+"""",
                ROUND(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions", 2) as "Total du nombre d'ETP",
                ROUND((etra."ETP Services généraux")/(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions"),2) as "Taux de personnel soins",
                ROUND((etra."ETP Socio-éducatif")/(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions"),2) as "Taux de personnel socio educatif",
                etra."Déficiences intellectuelles" AS "deficiences_intellectuelles",
                etra."Déficiences intellectuelles.1" AS "deficiences_intellectuelles1",
                etra."Déficiences auditives" AS "deficiences_auditives",
                etra."Déficiences auditives.1" AS "deficiences_auditives1",
                etra."Déficiences visuelles" AS "deficiences_visuelles",
                etra."Déficiences visuelles.1" AS "deficiences_visuelles1",
                etra."Déficiences motrices" AS "deficiences_motrices",
                etra."Déficiences motrices.1" AS "deficiences_motrices1",
                etra."Déficiences métaboliques, viscérales et nutritionnelles" AS "deficiences_metaboliques_viscerales_et_nutritionnelles",
                etra."Déficiences métaboliques, viscérales et nutritionnelles.1" AS "deficiences_metaboliques_viscerales_et_nutritionnelles1",
                etra."Autres types de déficiences" AS "autres_types_de_deficiences",
                etra."Autres types de déficiences.1" AS "autres_types_de_deficiences1",
                NULLTOZERO(rs.nb_recla) as "Nombre de réclamations sur la période"""+param_N_3+"""-"""+param_N+"""",
                NULLTOZERO(ROUND(CAST(rs.nb_recla AS FLOAT) / CAST(ccta.somme_de_capacite_autorisee_totale_ AS FLOAT), 4)*100) as "Rapport réclamations / capacité",
                NULLTOZERO(rs."Hôtellerie-locaux-restauration") as "Recla IGAS : Hôtellerie-locaux-restauration",
                NULLTOZERO(rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service") as "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
                NULLTOZERO(rs."Problème de qualité des soins médicaux") as "Recla IGAS : Problème de qualité des soins médicaux",
                NULLTOZERO(rs."Problème de qualité des soins paramédicaux") as "Recla IGAS : Problème de qualité des soins paramédicaux",
                NULLTOZERO(rs."Recherche d?établissement ou d?un professionnel") as "Recla IGAS : Recherche d’établissement ou d’un professionnel",
                NULLTOZERO(rs."Mise en cause attitude des professionnels") as "Recla IGAS : Mise en cause attitude des professionnels",
                NULLTOZERO(rs."Informations et droits des usagers") as "Recla IGAS : Informations et droits des usagers",
                NULLTOZERO(rs."Facturation et honoraires") as "Recla IGAS : Facturation et honoraires",
                NULLTOZERO(rs."Santé-environnementale") as "Recla IGAS : Santé-environnementale",
                NULLTOZERO(rs."Activités d?esthétique réglementées") as "Recla IGAS : Activités d’esthétique réglementées",
                NULLTOZERO(rs.nb_signa) as "Nombre de Signalement sur la période """+param_N_3+"""-"""+param_N+"""",
                NULLTOZERO(i.'ICE """+param_N_1+""" (réalisé)') as 'ICE """+param_N+""" (réalisé)',
                NULLTOZERO(i.'Inspection SUR SITE """+param_N_1+"""- Déjà réalisée') as 'Inspection SUR SITE """+param_N+""" - Déjà réalisée',
                NULLTOZERO(i.'Controle SUR PIECE """+param_N_1+"""- Déjà réalisé') as 'Controle SUR PIECE """+param_N+""" - Déjà réalisé',
                NULLTOZERO(i.'Inspection / contrôle Programmé """+param_N+"""') as 'Inspection / contrôle Programmé """+param_N+"""'
                FROM
                tfiness_clean tf 
                LEFT JOIN finess_pivoted fp on fp.finess=tf.finess
                LEFT JOIN errd_déficit_excédent_"""+param_N_4+""" on tf.finess= ede."FINESS de rattachement"
                LEFT JOIN errd_déficit_excédent_"""+param_N_3+""" on  tf.finess = ede2."FINESS de rattachement"
                LEFT JOIN errd_déficit_excédent_"""+param_N_2+""" on tf.finess = ede3."FINESS de rattachement"
                LEFT JOIN communes c on c.com = tf.com_code
                LEFT JOIN departement_"""+param_N_2+""" d on d.dep = c.dep
                LEFT JOIN region_"""+param_N_2+"""  r on d.reg = r.reg
                LEFT JOIN capacites_ehpad ce on ce."ET-N°FINESS" = tf.finess
                LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
                LEFT JOIN occupation_"""+param_N_5+"""_"""+param_N_4+""" o1 on o1.finess_19 = tf.finess
                LEFT JOIN occupation_"""+param_N_3+""" o2  on o2.finess = tf.finess
                LEFT JOIN clean_occupation_N_2 co3  on co3.finess = tf.finess
                LEFT JOIN clean_tdb_n_2 etra on etra.finess = tf.finess
                LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
                LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
                LEFT JOIN EHPAD_Indicateurs_"""+param_N_3+"""_REG_agg eira on eira.et_finess = tf.finess
                LEFT JOIN clean_tdb_n_4 d2 on SUBSTRING(d2.finess,1,9) = tf.finess
                LEFT JOIN clean_tdb_n_3 etra2 on etra2.finess = tf.finess
                LEFT JOIN clean_tdb_n_4 d2 on SUBSTRING(d2.finess,1,9) = tf.finess
                LEFT JOIN recla_signalement rs on rs.finess = tf.finess
                LEFT JOIN inspections i on i.finess = tf.finess
                WHERE r.reg = """+str(region)+"""
                ORDER BY tf.finess ASC"""
                cursor.execute(df_ciblage,(region,))
                res=cursor.fetchall()
                columns= [col[0] for col in cursor.description]
                df_ciblage= pd.DataFrame(res,columns=columns)
                print(df_ciblage)

                # Exécution requête controle HDF
                print('Exécution requête controle HDF')
                df_controle = f"""
                SELECT 
                r.ncc as Region,
                d.dep as "Code dép",
                d.ncc AS "Département",
                tf.categ_lib as Catégorie,
                tf.finess as "FINESS géographique",
                tf.rs as "Raison sociale ET",
                tf.ej_finess as "FINESS juridique",
                tf.ej_rs as "Raison sociale EJ",
                tf.statut_jur_lib as "Statut juridique",
                tf.adresse as Adresse,
                IIF(LENGTH(tf.adresse_code_postal) = 4, '0'|| tf.adresse_code_postal, tf.adresse_code_postal) AS "Code postal",
                c.NCC AS "Commune",
                IIF(LENGTH(tf.com_code) = 4, '0'|| tf.com_code, tf.com_code) AS "Code commune INSEE",
                CASE
                    WHEN tf.categ_code = 500
                    THEN CAST(NULLTOZERO(ce."TOTAL Héberg. Comp. Inter. Places Autorisées") as INTEGER) + CAST(NULLTOZERO(ce."TOTAL Accueil de Jour Places Autorisées") as INTEGER) + CAST(NULLTOZERO(ce."TOTAL Accueil de nuit Places Autorisées") as INTEGER)
                    ELSE CAST(ccta.somme_de_capacite_autorisee_totale_ as INTEGER)
                    END as "Capacité totale autorisée",
                fp."Type d'activité indifferencié" AS "Type d'activité indifferencié",
                fp."Protection Juridique" AS "Protection Juridique",
                fp."Hébergement Complet Internat" AS "Hébergement Complet Internat",
                fp."Placement Famille d'Accueil" AS "Placement Famille d'Accueil",
                fp."Accueil temporaire avec hébergement" AS "Accueil temporaire avec hébergement",
                fp."Prestation en milieu ordinaire" AS "Prestation en milieu ordinaire",
                fp."Accueil de jour et accompagnement en milieu ordinaire" AS "Accueil de jour et accompagnement en milieu ordinaire",
                fp."Accueil de Jour" AS "Accueil de Jour",
                fp."Accueil temporaire (avec et sans hébergement)" AS "Accueil temporaire (avec et sans hébergement)",
                fp."Semi-Internat" AS "Semi-Internat",
                fp."Hébergement de Nuit Eclaté" AS "Hébergement de Nuit Eclaté",
                fp."Internat de Semaine" AS "Internat de Semaine",
                fp."Externat" AS "Externat",
                fp."Tous modes d'accueil et d'accompagnement" AS "Tous modes d'accueil et d'accompagnement",
                fp."Tous modes d'accueil avec hébergement" AS "Tous modes d'accueil avec hébergement",
                fp."Accueil de Nuit" AS "Accueil de Nuit",
                fp."Permanence téléphonique" AS "Permanence téléphonique",
                fp."Accueil temporaire de jour" AS "Accueil temporaire de jour",
                fp."Consultation Soins Externes" AS "Consultation Soins Externes",
                fp."Accueil modulable/séquentiel" AS "Accueil modulable/séquentiel",
                fp."Traitement et Cure Ambulatoire" AS "Traitement et Cure Ambulatoire",
                fp."Tous modes d'accueil (avec et sans hébergement)" AS "Tous modes d'accueil (avec et sans hébergement)",
                fp."Equipe mobile de rue" AS "Equipe mobile de rue",
                fp."Accueil et prise en charge en appartement thérapeutique" AS "Accueil et prise en charge en appartement thérapeutique",
                fp."Hospitalisation Complète" AS "Hospitalisation Complète",
                fp."Regroupement Calcules (Annexes XXIV)" AS "Regroupement Calcules (Annexes XXIV)",
                fp."Aide Judiciaire à la Gestion du Budget Familial" AS "Aide Judiciaire à la Gestion du Budget Familial",
                fp."Accompagnement Social Personnalisé" AS "Accompagnement Social Personnalisé",
                fp."Information des Tuteurs Familiaux" AS "Information des Tuteurs Familiaux",
                fp."Administration" AS "Administration",
                o1.taux_occ_"""+param_N_4+""" AS "Taux d'occupation """+param_N_4+"""",
                o2.taux_occ_"""+param_N_3+""" AS "Taux d'occupation """+param_N_3+"""",
                co3.taux_occ_"""+param_N_2+""" AS "Taux d'occupation """+param_N_2+"""",
                co3.nb_lits_occ_"""+param_N_2+""" as "Nombre de résidents au 31/12/"""+param_N_2+"""",
                etra."Nombre total de chambres installées au 31.12" as "Nombre de places installées au 31/12/"""+param_N_2+"""",
                co3.taux_occ_trimestre3 AS "Taux occupation au 31/12/"""+param_N_2+"""",
                ROUND(CAST(REPLACE(eira."Taux_plus_10_médics_cip13", ",", ".") AS FLOAT),2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
                ROUND(CAST(REPLACE(eira.taux_atu, ",", ".") AS FLOAT),2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
                --"" as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
                CAST(REPLACE(taux_hospit_mco, ",", ".") AS FLOAT) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
                CAST(REPLACE(taux_hospit_had, ",", ".") AS FLOAT) as "Taux de recours à l'HAD des résidents d'EHPAD",
                ROUND(chpr."Total des charges") AS "Total des charges",
                ROUND(chpr."Produits de la tarification") AS "Produits de la tarification", 
                ROUND(chpr."Produits du compte 70") AS "Produits du compte 70",
                ROUND(chpr."Total des produits (hors c/775, 777, 7781 et 78)") AS "Total des produits (hors c/775, 777, 7781 et 78)",
                "deficit ou excédent" AS "deficit ou excédent """+param_N_4+""" ",
                "deficit ou excédent" AS "deficit ou excédent """+param_N_3+""" ",
                "deficit ou excédent" AS "deficit ou excédent """+param_N_2+""" ",
                "" as "Saisie des indicateurs du TDB MS (campagne """+param_N_2+""")",
                CAST(d2."Taux d'absentéisme (hors formation) en %" as decmail) as "Taux d'absentéisme """+param_N_4+"""",
                etra2."Taux d'absentéisme (hors formation) en %"     as "Taux d'absentéisme """+param_N_3+"""",
                etra."Taux d'absentéisme (hors formation) en %" as "Taux d'absentéisme """+param_N_2+"""",
                ROUND(MOY3(d2."Taux d'absentéisme (hors formation) en %" ,etra2."Taux d'absentéisme (hors formation) en %"     , etra."Taux d'absentéisme (hors formation) en %") ,2) as "Absentéisme moyen sur la période """+param_N_4+"""-"""+param_N_2+"""",
                CAST(d2."Taux de rotation des personnels" as decimal) as "Taux de rotation du personnel titulaire """+param_N_4+"""",
                etra2."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_3+"""",
                etra."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_2+"""",
                ROUND(MOY3(d2."Taux de rotation des personnels" , etra2."Taux de rotation des personnels" , etra."Taux de rotation des personnels"), 2) as "Rotation moyenne du personnel sur la période """+param_N_4+"""-"""+param_N_2+"""",
                CAST(d2."Taux d'ETP vacants" as decimal) as "ETP vacants """+param_N_4+"""",
                etra2."Taux d'ETP vacants"  as "ETP vacants """+param_N_3+"""",
                etra."Taux d'ETP vacants" as "ETP vacants """+param_N_2+"""",
                etra."Dont taux d'ETP vacants concernant la fonction SOINS" as "dont fonctions soins """+param_N_2+"""",
                etra."Dont taux d'ETP vacants concernant la fonction SOCIO EDUCATIVE" as "dont fonctions socio-éducatives """+param_N_2+"""", 
                CAST(REPLACE(d2."Taux de prestations externes sur les prestations directes",',','.')as decimal) as "Taux de prestations externes sur les prestations directes """+param_N_4+"""",
                etra2."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes """+param_N_3+"""", 
                etra."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes """+param_N_2+"""",
                ROUND(MOY3(d2."Taux de prestations externes sur les prestations directes" , etra2."Taux de prestations externes sur les prestations directes" , etra."Taux de prestations externes sur les prestations directes") ,2) as "Taux moyen de prestations externes sur les prestations directes",
                ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_4+"""",
                ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) as "Nombre total d'ETP par usager en """+param_N_3+"""",
                ROUND((etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_2+"""",
                MOY3(ROUND(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) , ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) , ROUND((etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2),2))AS "Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+"""",
                ROUND((etra."ETP Paramédical" + etra."ETP Médical")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "ETP 'soins' par usager en """+param_N_2+"""",
                etra."ETP Direction/Encadrement" AS "Direction / Encadrement",
                etra."- Dont nombre d'ETP réels de personnel médical d'encadrement" AS "dont personnel médical d'encadrement",
                etra."_dont_autre_directionencadrement" AS "dont autre Direction / Encadrement",
                etra."ETP Administration /Gestion" AS "Administration / Gestion",
                etra."ETP Services généraux" AS "Services généraux",
                etra."ETP Restauration" AS "Restauration",
                etra."ETP Socio-éducatif" AS "Socio-éducatif",
                etra."- Dont nombre d'ETP réels d'aide médico-psychologique" AS "dont AMP",
                etra."- Dont nombre d'ETP réels d'animateur" AS "dont animateur",
                etra."- Dont nombre d'ETP réels de moniteur éducateur au 31.12" AS "dont moniteur éducateur",
                etra."- Dont nombre d’ETP réels d’éducateur spécialisé au 31.12" AS "dont éducateur spécialisé",
                etra."- Dont nombre d’ETP réels d’assistant social au 31.12" AS "dont assistant(e) social(e)",
                etra."-_dont_autre_socio-educatif" AS "dont autre socio-éducatif",
                etra."ETP Paramédical" AS "Paramédical",
                etra."- Dont nombre d'ETP réels d'infirmier" AS "dont infirmier",
                etra."- Dont nombre d'ETP réels d'aide médico-psychologique.1" AS "dont AMP",
                etra."- Dont nombre d'ETP réels d'aide soignant" AS "dont aide-soignant(e) ",
                etra."- Dont nombre d'ETP réels de kinésithérapeute" AS "dont kinésithérapeute",
                etra."- Dont nombre d'ETP réels de psychomotricien" AS "dont psychomotricien(ne)",
                etra."- Dont nombre d'ETP réels d'ergothérapeute" AS "dont ergothérapeute",
                etra."- Dont nombre d'ETP réels d'orthophoniste" AS "dont orthophoniste",
                etra."-_dont_autre_paramedical" AS "dont autre paramédical",
                etra."ETP Psychologue" AS "Psychologue",
                etra."ETP ASH" AS "ASH",
                etra."ETP Médical" AS "Médical",
                etra."- Dont nombre d'ETP réels de médecin coordonnateur" as "dont médecin coordonnateur",
                etra."-_dont_autre_medical" AS "dont autre médical",
                etra."ETP Personnel Education nationale" AS "Personnel éducation nationale",
                etra."ETP Autres fonctions" AS "Autres fonctions",
                ROUND(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions", 2) as "Total du nombre d'ETP",
                ROUND((etra."ETP Services généraux")/(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions"),2) as "Taux de personnel soins",
                ROUND((etra."ETP Socio-éducatif")/(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions"),2) as "Taux de personnel socio educatif",
                etra."Déficiences intellectuelles" AS "deficiences_intellectuelles",
                etra."Déficiences intellectuelles.1" AS "deficiences_intellectuelles1",
                etra."Déficiences auditives" AS "deficiences_auditives",
                etra."Déficiences auditives.1" AS "deficiences_auditives1",
                etra."Déficiences visuelles" AS "deficiences_visuelles",
                etra."Déficiences visuelles.1" AS "deficiences_visuelles1",
                etra."Déficiences motrices" AS "deficiences_motrices",
                etra."Déficiences motrices.1" AS "deficiences_motrices1",
                etra."Déficiences métaboliques, viscérales et nutritionnelles" AS "deficiences_metaboliques_viscerales_et_nutritionnelles",
                etra."Déficiences métaboliques, viscérales et nutritionnelles.1" AS "deficiences_metaboliques_viscerales_et_nutritionnelles1",
                etra."Autres types de déficiences" AS "autres_types_de_deficiences",
                etra."Autres types de déficiences.1" AS "autres_types_de_deficiences1",
                SUM(etra."_de_personnes_agees_de_20_-_29_ans" +
                etra."_de_personnes_agees_de_30_-_39_ans" +
                etra."_de_personnes_agees_de_40_-_49_ans" +
                etra."_de_personnes_agees_de_50_-_54_ans" +
                etra."_de_personnes_agees_de_55_-_59_ans" +
                etra."_de_personnes_agees_de_60_-_64_ans" +
                etra."_de_personnes_agees_de_plus_de_65_ans") AS Taux de résidents de plus de 20 ans , 
                etra."Taux de vétusté des constructions" AS Taux de vétusté des constructions,
                etra."Taux de vétusté des équipements en %" AS "Taux de vétusté des équipements en %",
                NULLTOZERO(rs.nb_recla) as "Nombre de réclamations sur la période"""+param_N_3+"""-"""+param_N+"""",
                NULLTOZERO(ROUND(CAST(rs.nb_recla AS FLOAT) / CAST(ccta.somme_de_capacite_autorisee_totale_ AS FLOAT), 4)*100) as "Rapport réclamations / capacité",
                NULLTOZERO(rs."Hôtellerie-locaux-restauration") as "Recla IGAS : Hôtellerie-locaux-restauration",
                NULLTOZERO(rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service") as "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
                NULLTOZERO(rs."Problème de qualité des soins médicaux") as "Recla IGAS : Problème de qualité des soins médicaux",
                NULLTOZERO(rs."Problème de qualité des soins paramédicaux") as "Recla IGAS : Problème de qualité des soins paramédicaux",
                NULLTOZERO(rs."Recherche d?établissement ou d?un professionnel") as "Recla IGAS : Recherche d’établissement ou d’un professionnel",
                NULLTOZERO(rs."Mise en cause attitude des professionnels") as "Recla IGAS : Mise en cause attitude des professionnels",
                NULLTOZERO(rs."Informations et droits des usagers") as "Recla IGAS : Informations et droits des usagers",
                NULLTOZERO(rs."Facturation et honoraires") as "Recla IGAS : Facturation et honoraires",
                NULLTOZERO(rs."Santé-environnementale") as "Recla IGAS : Santé-environnementale",
                NULLTOZERO(rs."Activités d?esthétique réglementées") as "Recla IGAS : Activités d’esthétique réglementées",
                NULLTOZERO(rs.nb_signa) as "Nombre de Signalement sur la période """+param_N_3+"""-"""+param_N+"""",
                NULLTOZERO(i.'ICE """+param_N_1+""" (réalisé)') as 'ICE """+param_N+""" (réalisé)',
                NULLTOZERO(i.'Inspection SUR SITE """+param_N_1+"""- Déjà réalisée') as 'Inspection SUR SITE """+param_N+""" - Déjà réalisée',
                NULLTOZERO(i.'Controle SUR PIECE """+param_N_1+"""- Déjà réalisé') as 'Controle SUR PIECE """+param_N+""" - Déjà réalisé',
                NULLTOZERO(i.'Inspection / contrôle Programmé """+param_N+"""') as 'Inspection / contrôle Programmé """+param_N+"""'
                FROM
                tfiness_clean tf 
                LEFT JOIN finess_pivoted fp on fp.finess=tf.finess
                LEFT JOIN [errd_déficit_excédent_"""+param_N_4+"""] ede on tf.finess= SUBSTRING(ede."Structure - FINESS - RAISON SOCIALE", 1, 9)
                LEFT JOIN [errd_déficit_excédent_"""+param_N_3+"""] ede2 on  tf.finess = SUBSTRING(ede2."Structure - FINESS - RAISON SOCIALE", 1, 9)
                LEFT JOIN [errd_déficit_excédent_"""+param_N_2+"""] ede3 on tf.finess = SUBSTRING(ede3."Structure - FINESS - RAISON SOCIALE", 1, 9)
                LEFT JOIN communes c on c.com = tf.com_code
                LEFT JOIN departement_"""+param_N_2+""" d on d.dep = c.dep
                LEFT JOIN region_"""+param_N_2+"""  r on d.reg = r.reg
                LEFT JOIN capacites_ehpad ce on ce."ET-N°FINESS" = tf.finess
                LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
                LEFT JOIN occupation_"""+param_N_5+"""_"""+param_N_4+""" o1 on o1.finess_19 = tf.finess
                LEFT JOIN occupation_"""+param_N_3+""" o2  on o2.finess = tf.finess
                LEFT JOIN clean_occupation_N_2 co3  on co3.finess = tf.finess
                LEFT JOIN clean_tdb_n_2 etra on etra.finess = tf.finess
                LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
                LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
                LEFT JOIN EHPAD_Indicateurs_"""+param_N_3+"""_REG_agg eira on eira.et_finess = tf.finess
                LEFT JOIN clean_tdb_n_4 d2 on SUBSTRING(d2.finess,1,9) = tf.finess
                LEFT JOIN clean_tdb_n_3 etra2 on etra2.finess = tf.finess
                LEFT JOIN clean_tdb_n_4 d2 on SUBSTRING(d2.finess,1,9) = tf.finess
                LEFT JOIN recla_signalement rs on rs.finess = tf.finess
                LEFT JOIN inspections i on i.finess = tf.finess
                WHERE r.reg = """+str(region)+"""
                ORDER BY tf.finess ASC"""
                cursor.execute(df_controle,(region,))
                res=cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                df_controle=pd.DataFrame(res,columns=columns) 
                
            else: 

                # Exécution requête ciblage
                print('Exécution requête ciblage')
                df_ciblage = f"""
                SELECT 
                r.ncc as Region,
                d.dep as "Code dép",
                d.ncc AS "Département",
                tf.categ_lib as Catégorie,
                tf.finess as "FINESS géographique",
                tf.rs as "Raison sociale ET",
                tf.ej_finess as "FINESS juridique",
                tf.ej_rs as "Raison sociale EJ",
                tf.statut_jur_lib as "Statut juridique",
                tf.adresse as Adresse,
                IIF(LENGTH(tf.adresse_code_postal) = 4, '0'|| tf.adresse_code_postal, tf.adresse_code_postal) AS "Code postal",
                c.NCC AS "Commune",
                IIF(LENGTH(tf.com_code) = 4, '0'|| tf.com_code, tf.com_code) AS "Code commune INSEE",
                CASE
                WHEN tf.categ_code = 500
                    THEN CAST(NULLTOZERO(ce."TOTAL Héberg. Comp. Inter. Places Autorisées") as INTEGER) + CAST(NULLTOZERO(ce."TOTAL Accueil de Jour Places Autorisées") as INTEGER) + CAST(NULLTOZERO(ce."TOTAL Accueil de nuit Places Autorisées") as INTEGER)
                    ELSE CAST(ccta.somme_de_capacite_autorisee_totale_ as INTEGER)
                END as "Capacité totale autorisée",
                fp."Type d'activité indifferencié" AS "Type d'activité indifferencié",
                fp."Protection Juridique" AS "Protection Juridique",
                fp."Hébergement Complet Internat" AS "Hébergement Complet Internat",
                fp."Placement Famille d'Accueil" AS "Placement Famille d'Accueil",
                fp."Accueil temporaire avec hébergement" AS "Accueil temporaire avec hébergement",
                fp."Prestation en milieu ordinaire" AS "Prestation en milieu ordinaire",
                fp."Accueil de jour et accompagnement en milieu ordinaire" AS "Accueil de jour et accompagnement en milieu ordinaire",
                fp."Accueil de Jour" AS "Accueil de Jour",
                fp."Accueil temporaire (avec et sans hébergement)" AS "Accueil temporaire (avec et sans hébergement)",
                fp."Semi-Internat" AS "Semi-Internat",
                fp."Hébergement de Nuit Eclaté" AS "Hébergement de Nuit Eclaté",
                fp."Internat de Semaine" AS "Internat de Semaine",
                fp."Externat" AS "Externat",
                fp."Tous modes d'accueil et d'accompagnement" AS "Tous modes d'accueil et d'accompagnement",
                fp."Tous modes d'accueil avec hébergement" AS "Tous modes d'accueil avec hébergement",
                fp."Accueil de Nuit" AS "Accueil de Nuit",
                fp."Permanence téléphonique" AS "Permanence téléphonique",
                fp."Accueil temporaire de jour" AS "Accueil temporaire de jour",
                fp."Consultation Soins Externes" AS "Consultation Soins Externes",
                fp."Accueil modulable/séquentiel" AS "Accueil modulable/séquentiel",
                fp."Traitement et Cure Ambulatoire" AS "Traitement et Cure Ambulatoire",
                fp."Tous modes d'accueil (avec et sans hébergement)" AS "Tous modes d'accueil (avec et sans hébergement)",
                fp."Equipe mobile de rue" AS "Equipe mobile de rue",
                fp."Accueil et prise en charge en appartement thérapeutique" AS "Accueil et prise en charge en appartement thérapeutique",
                fp."Hospitalisation Complète" AS "Hospitalisation Complète",
                fp."Regroupement Calcules (Annexes XXIV)" AS "Regroupement Calcules (Annexes XXIV)",
                fp."Aide Judiciaire à la Gestion du Budget Familial" AS "Aide Judiciaire à la Gestion du Budget Familial",
                fp."Accompagnement Social Personnalisé" AS "Accompagnement Social Personnalisé",
                fp."Information des Tuteurs Familiaux" AS "Information des Tuteurs Familiaux",
                fp."Administration" AS "Administration",
                co3.nb_lits_occ_"""+param_N_2+""" as "Nombre de résidents au 31/12/"""+param_N_2+"""",
                etra."Nombre total de chambres installées au 31.12" as "Nombre de places installées au 31/12/"""+param_N_2+"""",
                ROUND(CAST(REPLACE(eira."Taux_plus_10_médics_cip13", ",", ".") AS FLOAT),2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
                ROUND(CAST(REPLACE(eira.taux_atu, ",", ".") AS FLOAT),2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
                ROUND(CAST(REPLACE(taux_hospit_mco, ",", ".") AS FLOAT),2) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
                ROUND(CAST(REPLACE(taux_hospit_had, ",", ".") AS FLOAT),2) as "Taux de recours à l'HAD des résidents d'EHPAD",
                ede."deficit ou excédent" AS "deficit ou excédent """+param_N_4+""" ",
                ede2."deficit ou excédent" AS "deficit ou excédent """+param_N_3+""" ",
                ede3."deficit ou excédent" AS "deficit ou excédent """+param_N_2+""" ",
                "" as "Saisie des indicateurs du TDB MS (campagne """+param_N_2+""")",
                CAST(d2."Taux d'absentéisme (hors formation) en %" as decmail) as "Taux d'absentéisme """+param_N_4+"""",
                etra2."Taux d'absentéisme (hors formation) en %"     as "Taux d'absentéisme """+param_N_3+"""",
                etra."Taux d'absentéisme (hors formation) en %" as "Taux d'absentéisme """+param_N_2+"""",
                ROUND(MOY3(d2."Taux d'absentéisme (hors formation) en %" ,etra2."Taux d'absentéisme (hors formation) en %"    , etra."Taux d'absentéisme (hors formation) en %") ,2) as "Absentéisme moyen sur la période """+param_N_4+"""-"""+param_N_2+"""",
                CAST(d2."Taux de rotation des personnels" as decimal) as "Taux de rotation du personnel titulaire """+param_N_4+"""",
                etra2."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_3+"""",
                etra."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_2+"""",
                ROUND(MOY3(d2."Taux de rotation des personnels" , etra2."Taux de rotation des personnels" , etra."Taux de rotation des personnels"), 2) as "Rotation moyenne du personnel sur la période """+param_N_4+"""-"""+param_N_2+"""",
                CAST(d2."Taux d'ETP vacants" as decimal) as "ETP vacants """+param_N_4+"""",
                etra2."Taux d'ETP vacants"  as "ETP vacants """+param_N_3+"""",
                etra."Taux d'ETP vacants" as "ETP vacants """+param_N_2+"""",
                etra."Dont taux d'ETP vacants concernant la fonction SOINS" as "dont fonctions soins """+param_N_2+"""",
                etra."Dont taux d'ETP vacants concernant la fonction SOCIO EDUCATIVE" as "dont fonctions socio-éducatives """+param_N_2+"""", 
                CAST(REPLACE(d2."Taux de prestations externes sur les prestations directes",',','.')as decimal) as "Taux de prestations externes sur les prestations directes """+param_N_4+"""",
                etra2."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes """+param_N_3+"""", 
                etra."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes"""+param_N_2+"""",
                ROUND(MOY3(d2."Taux de prestations externes sur les prestations directes" , etra2."Taux de prestations externes sur les prestations directes" , etra."Taux de prestations externes sur les prestations directes") ,2) as "Taux moyen de prestations externes sur les prestations directes",
                ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_4+"""",
                ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) as "Nombre total d'ETP par usager en """+param_N_3+"""",
                ROUND((etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_2+"""",
                ROUND(MOY3(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) , ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) , 
                ROUND((etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)),2)AS "Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+"""",
                ROUND((etra."ETP Paramédical" + etra."ETP Médical")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "ETP 'soins' par usager en """+param_N_2+"""",
                etra."- Dont nombre d'ETP réels de médecin coordonnateur" as "dont médecin coordonnateur",
                ROUND(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions", 2) as "Total du nombre d'ETP",
                ROUND((etra."ETP Services généraux")/(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions"),2) as "Taux de personnel soins",
                ROUND((etra."ETP Socio-éducatif")/(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions"),2) as "Taux de personnel socio educatif",
                etra."Déficiences intellectuelles" AS "deficiences_intellectuelles",
                etra."Déficiences intellectuelles.1" AS "deficiences_intellectuelles1",
                etra."Déficiences auditives" AS "deficiences_auditives",
                etra."Déficiences auditives.1" AS "deficiences_auditives1",
                etra."Déficiences visuelles" AS "deficiences_visuelles",
                etra."Déficiences visuelles.1" AS "deficiences_visuelles1",
                etra."Déficiences motrices" AS "deficiences_motrices",
                etra."Déficiences motrices.1" AS "deficiences_motrices1",
                etra."Déficiences métaboliques, viscérales et nutritionnelles" AS "deficiences_metaboliques_viscerales_et_nutritionnelles",
                etra."Déficiences métaboliques, viscérales et nutritionnelles.1" AS "deficiences_metaboliques_viscerales_et_nutritionnelles1",
                etra."Autres types de déficiences" AS "autres_types_de_deficiences",
                etra."Autres types de déficiences.1" AS "autres_types_de_deficiences1",
                NULLTOZERO(rs.nb_recla) as "Nombre de réclamations sur la période"""+param_N_3+"""-"""+param_N+"""",
                NULLTOZERO(ROUND(CAST(rs.nb_recla AS FLOAT) / CAST(ccta.somme_de_capacite_autorisee_totale_ AS FLOAT), 4)*100) as "Rapport réclamations / capacité",
                NULLTOZERO(rs."Hôtellerie-locaux-restauration") as "Recla IGAS : Hôtellerie-locaux-restauration",
                NULLTOZERO(rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service") as "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
                NULLTOZERO(rs."Problème de qualité des soins médicaux") as "Recla IGAS : Problème de qualité des soins médicaux",
                NULLTOZERO(rs."Problème de qualité des soins paramédicaux") as "Recla IGAS : Problème de qualité des soins paramédicaux",
                NULLTOZERO(rs."Recherche d?établissement ou d?un professionnel") as "Recla IGAS : Recherche d’établissement ou d’un professionnel",
                NULLTOZERO(rs."Mise en cause attitude des professionnels") as "Recla IGAS : Mise en cause attitude des professionnels",
                NULLTOZERO(rs."Informations et droits des usagers") as "Recla IGAS : Informations et droits des usagers",
                NULLTOZERO(rs."Facturation et honoraires") as "Recla IGAS : Facturation et honoraires",
                NULLTOZERO(rs."Santé-environnementale") as "Recla IGAS : Santé-environnementale",
                NULLTOZERO(rs."Activités d?esthétique réglementées") as "Recla IGAS : Activités d’esthétique réglementées",
                NULLTOZERO(rs.NB_EIGS) as "Nombre d'EIG sur la période """+param_N_3+"""-"""+param_N+"""",
                NULLTOZERO(rs.NB_EIAS) as "Nombre d'EIAS sur la période """+param_N_3+"""-"""+param_N+"""",
                NULLTOZERO(rs."Nombre d'EI sur la période 36mois") + NULLTOZERO(rs.NB_EIGS) + NULLTOZERO(rs.NB_EIAS) as "Somme EI + EIGS + EIAS sur la période """+param_N_3+"""-"""+param_N_1+"""",
                NULLTOZERO(i.'ICE """+param_N_1+""" (réalisé)') as 'ICE """+param_N+""" (réalisé)',
                NULLTOZERO(i.'Inspection SUR SITE """+param_N_1+"""- Déjà réalisée') as 'Inspection SUR SITE """+param_N+""" - Déjà réalisée',
                NULLTOZERO(i.'Controle SUR PIECE """+param_N_1+"""- Déjà réalisé') as 'Controle SUR PIECE """+param_N+""" - Déjà réalisé',
                NULLTOZERO(i.'Inspection / contrôle Programmé """+param_N+"""') as 'Inspection / contrôle Programmé """+param_N+"""'
                FROM tfiness_clean tf 
                LEFT JOIN finess_pivoted fp on fp.finess= tf.finess
                LEFT JOIN [errd_déficit_excédent_"""+param_N_4+"""] ede on tf.finess= SUBSTRING(ede."Structure - FINESS - RAISON SOCIALE", 1, 9)
                LEFT JOIN [errd_déficit_excédent_"""+param_N_3+"""] ede2 on  tf.finess = SUBSTRING(ede2."Structure - FINESS - RAISON SOCIALE", 1, 9)
                LEFT JOIN [errd_déficit_excédent_"""+param_N_2+"""] ede3 on tf.finess = SUBSTRING(ede3."Structure - FINESS - RAISON SOCIALE", 1, 9)
                LEFT JOIN communes c on c.com = tf.com_code
                LEFT JOIN departement_"""+param_N_2+""" d on d.dep = c.dep
                LEFT JOIN region_"""+param_N_2+""" r on d.reg = r.reg
                LEFT JOIN capacites_ehpad ce on ce."ET-N°FINESS" = tf.finess
                LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
                LEFT JOIN occupation_"""+param_N_5+"""_"""+param_N_4+"""  o1 on o1.finess_19 = tf.finess
                LEFT JOIN occupation_"""+param_N_3+"""  o2  on o2.finess = tf.finess
                LEFT JOIN clean_occupation_N_2  co3  on co3.finess = tf.finess
                LEFT JOIN clean_tdb_n_2  etra on etra.finess = tf.finess
                LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
                LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
                LEFT JOIN EHPAD_Indicateurs_"""+param_N_3+"""_REG_agg eira on eira.et_finess = tf.finess
                LEFT JOIN clean_tdb_n_4  d2 on d2.finess = tf.finess
                LEFT JOIN clean_tdb_n_3  etra2 on etra2.finess = tf.finess
                LEFT JOIN recla_signalement rs on rs.finess = tf.finess
                LEFT JOIN inspections i on i.finess = tf.finess
                WHERE r.reg ='"""+str(region)+"""'
                GROUP BY tf.finess """
                cursor.execute(df_ciblage)
                res=cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                df_ciblage=pd.DataFrame(res,columns=columns)

            # Exécution requête controle
            print('Exécution requête controle')
            df_controle = f"""
            SELECT 
            r.ncc as Region,
            d.dep as "Code dép",
            d.ncc AS "Département",
            tf.categ_lib as "Catégorie",
            tf.finess as "FINESS géographique",
            tf.rs as "Raison sociale ET",
            tf.ej_finess as "FINESS juridique",
            tf.ej_rs as "Raison sociale EJ",
            tf.statut_jur_lib as "Statut juridique",
            tf.adresse as Adresse,
            IIF(LENGTH(tf.adresse_code_postal) = 4, '0'|| tf.adresse_code_postal, tf.adresse_code_postal) AS "Code postal",
            c.NCC AS "Commune",
            IIF(LENGTH(tf.com_code) = 4, '0'|| tf.com_code, tf.com_code) AS "Code commune INSEE",
            CASE
                WHEN tf.categ_code = 500
                    THEN CAST(NULLTOZERO(ce."TOTAL Héberg. Comp. Inter. Places Autorisées") as INTEGER) + CAST(NULLTOZERO(ce."TOTAL Accueil de Jour Places Autorisées") as INTEGER) + CAST(NULLTOZERO(ce."TOTAL Accueil de nuit Places Autorisées") as INTEGER)
                ELSE CAST(ccta.somme_de_capacite_autorisee_totale_ as INTEGER)
            END as "Capacité totale autorisée",
                fp."Type d'activité indifferencié" AS "Type d'activité indifferencié",
                fp."Protection Juridique" AS "Protection Juridique",
                fp."Hébergement Complet Internat" AS "Hébergement Complet Internat",
                fp."Placement Famille d'Accueil" AS "Placement Famille d'Accueil",
                fp."Accueil temporaire avec hébergement" AS "Accueil temporaire avec hébergement",
                fp."Prestation en milieu ordinaire" AS "Prestation en milieu ordinaire",
                fp."Accueil de jour et accompagnement en milieu ordinaire" AS "Accueil de jour et accompagnement en milieu ordinaire",
                fp."Accueil de Jour" AS "Accueil de Jour",
                fp."Accueil temporaire (avec et sans hébergement)" AS "Accueil temporaire (avec et sans hébergement)",
                fp."Semi-Internat" AS "Semi-Internat",
                fp."Hébergement de Nuit Eclaté" AS "Hébergement de Nuit Eclaté",
                fp."Internat de Semaine" AS "Internat de Semaine",
                fp."Externat" AS "Externat",
                fp."Tous modes d'accueil et d'accompagnement" AS "Tous modes d'accueil et d'accompagnement",
                fp."Tous modes d'accueil avec hébergement" AS "Tous modes d'accueil avec hébergement",
                fp."Accueil de Nuit" AS "Accueil de Nuit",
                fp."Permanence téléphonique" AS "Permanence téléphonique",
                fp."Accueil temporaire de jour" AS "Accueil temporaire de jour",
                fp."Consultation Soins Externes" AS "Consultation Soins Externes",
                fp."Accueil modulable/séquentiel" AS "Accueil modulable/séquentiel",
                fp."Traitement et Cure Ambulatoire" AS "Traitement et Cure Ambulatoire",
                fp."Tous modes d'accueil (avec et sans hébergement)" AS "Tous modes d'accueil (avec et sans hébergement)",
                fp."Equipe mobile de rue" AS "Equipe mobile de rue",
                fp."Accueil et prise en charge en appartement thérapeutique" AS "Accueil et prise en charge en appartement thérapeutique",
                fp."Hospitalisation Complète" AS "Hospitalisation Complète",
                fp."Regroupement Calcules (Annexes XXIV)" AS "Regroupement Calcules (Annexes XXIV)",
                fp."Aide Judiciaire à la Gestion du Budget Familial" AS "Aide Judiciaire à la Gestion du Budget Familial",
                fp."Accompagnement Social Personnalisé" AS "Accompagnement Social Personnalisé",
                fp."Information des Tuteurs Familiaux" AS "Information des Tuteurs Familiaux",
                fp."Administration" AS "Administration",
            o1.taux_occ_"""+param_N_4+"""  AS "Taux d'occupation """+param_N_4+""" ",
            o2.taux_occ_"""+param_N_3+"""  AS "Taux d'occupation """+param_N_3+""" ",
            co3.taux_occ_"""+param_N_2+"""  AS "Taux d'occupation """+param_N_2+""" ",
            co3.nb_lits_occ_"""+param_N_2+"""  as "Nombre de résidents au 31/12/"""+param_N_2+""" ",
            etra."Nombre total de chambres installées au 31.12" as "Nombre de places installées au 31/12/"""+param_N_2+""" ",
            co3.taux_occ_trimestre3 AS "Taux occupation au 31/12/"""+param_N_2+""" ",
            ROUND(CAST(REPLACE(eira."Taux_plus_10_médics_cip13", ",", ".") AS FLOAT),2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
            ROUND(CAST(REPLACE(eira.taux_atu, ",", ".") AS FLOAT),2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
            ROUND(CAST(REPLACE(taux_hospit_mco, ",", ".") AS FLOAT),2) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
            ROUND(CAST(REPLACE(taux_hospit_had, ",", ".") AS FLOAT),2) as "Taux de recours à l'HAD des résidents d'EHPAD",
            ROUND(chpr."Total des charges") AS "Total des charges",
            ROUND(chpr."Produits de la tarification") AS "Produits de la tarification", 
            ROUND(chpr."Produits du compte 70") AS "Produits du compte 70",
            ROUND(chpr."Total des produits (hors c/775, 777, 7781 et 78)") AS "Total des produits (hors c/775, 777, 7781 et 78)",
            ede."deficit ou excédent" AS "deficit ou excédent """+param_N_4+""" ",
            ede2."deficit ou excédent" AS "deficit ou excédent """+param_N_3+""" ",
            ede3."deficit ou excédent" AS "deficit ou excédent """+param_N_2+""" ",
            "" as "Saisie des indicateurs du TDB MS (campagne """+param_N_2+""" )",
            CAST(d2."Taux d'absentéisme (hors formation) en %" as decimal) as "Taux d'absentéisme """+param_N_4+""" ",
            etra2."Taux d'absentéisme (hors formation) en %"    as "Taux d'absentéisme """+param_N_3+""" ",
            etra."Taux d'absentéisme (hors formation) en %" as "Taux d'absentéisme """+param_N_2+""" ",
            ROUND(MOY3(d2."Taux d'absentéisme (hors formation) en %" ,etra2."Taux d'absentéisme (hors formation) en %"    , etra."Taux d'absentéisme (hors formation) en %") ,2) as "Absentéisme moyen sur la période """+param_N_4+""" -"""+param_N_2+""" ",
            CAST(d2."Taux de rotation des personnels" as decimal) as "Taux de rotation du personnel titulaire """+param_N_4+""" ",
            etra2."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_3+""" ",
            etra."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_2+""" ",
            ROUND(MOY3(d2."Taux de rotation des personnels" , etra2."Taux de rotation des personnels" , etra."Taux de rotation des personnels"), 2) as "Rotation moyenne du personnel sur la période """+param_N_4+""" -"""+param_N_2+""" ",
            CAST(d2."Taux d'ETP vacants" as decimal) as "ETP vacants """+param_N_4+""" ",
            etra2."Taux d'ETP vacants"  as "ETP vacants """+param_N_3+""" ",
            etra."Taux d'ETP vacants" as "ETP vacants """+param_N_2+""" ",
            etra."Dont taux d'ETP vacants concernant la fonction SOINS" as "dont fonctions soins """+param_N_2+""" ",
            etra."Dont taux d'ETP vacants concernant la fonction SOCIO EDUCATIVE" as "dont fonctions socio-éducatives """+param_N_2+""" ", 
            CAST(REPLACE(d2."Taux de prestations externes sur les prestations directes",',','.')as decimal) as "Taux de prestations externes sur les prestations directes """+param_N_4+""" ",
            etra2."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes """+param_N_3+""" ", 
            etra."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes """+param_N_2+""" ",
            ROUND(MOY3(d2."Taux de prestations externes sur les prestations directes" , etra2."Taux de prestations externes sur les prestations directes" , etra."Taux de prestations externes sur les prestations directes") ,2) as "Taux moyen de prestations externes sur les prestations directes",
            ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_4+""" ",
            ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) as "Nombre total d'ETP par usager en """+param_N_3+""" ",
            ROUND((etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_2+""" ",
            ROUND(MOY3(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) , ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) , ROUND((etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)),2)AS "Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+""" ",
            ROUND((etra."ETP Paramédical" + etra."ETP Médical")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "ETP 'soins' par usager en """+param_N_2+""" ",
            etra."ETP Direction/Encadrement" AS "Direction / Encadrement",
            etra."- Dont nombre d'ETP réels de personnel médical d'encadrement" AS "dont personnel médical d'encadrement",
            etra."_dont_autre_directionencadrement" AS "dont autre Direction / Encadrement",
            etra."ETP Administration /Gestion" AS "Administration / Gestion",
            etra."ETP Services généraux" AS "Services généraux",
            etra."ETP Restauration" AS "Restauration",
            etra."ETP Socio-éducatif" AS "Socio-éducatif",
            etra."- Dont nombre d'ETP réels d'aide médico-psychologique" AS "dont AMP",
            etra."- Dont nombre d'ETP réels d'animateur" AS "dont animateur",
            etra."- Dont nombre d'ETP réels de moniteur éducateur au 31.12" AS "dont moniteur éducateur",
            etra."- Dont nombre d’ETP réels d’éducateur spécialisé au 31.12" AS "dont éducateur spécialisé",
            etra."- Dont nombre d’ETP réels d’assistant social au 31.12" AS "dont assistant(e) social(e)",
            etra."-_dont_autre_socio-educatif" AS "dont autre socio-éducatif",
            etra."ETP Paramédical" AS "Paramédical",
            etra."- Dont nombre d'ETP réels d'infirmier" AS "dont infirmier",
            etra."- Dont nombre d'ETP réels d'aide médico-psychologique.1" AS "dont AMP",
            etra."- Dont nombre d'ETP réels d'aide soignant" AS "dont aide-soignant(e) ",
            etra."- Dont nombre d'ETP réels de kinésithérapeute" AS "dont kinésithérapeute",
            etra."- Dont nombre d'ETP réels de psychomotricien" AS "dont psychomotricien(ne)",
            etra."- Dont nombre d'ETP réels d'ergothérapeute" AS "dont ergothérapeute",
            etra."- Dont nombre d'ETP réels d'orthophoniste" AS "dont orthophoniste",
            etra."-_dont_autre_paramedical" AS "dont autre paramédical",
            etra."ETP Psychologue" AS "Psychologue",
            etra."ETP ASH" AS "ASH",
            etra."ETP Médical" AS "Médical",
            etra."- Dont nombre d'ETP réels de médecin coordonnateur" as "dont médecin coordonnateur",
            etra."-_dont_autre_medical" AS "dont autre médical",
            etra."ETP Personnel Education nationale" AS "Personnel éducation nationale",
            etra."ETP Autres fonctions" AS "Autres fonctions",
            ROUND(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions", 2) as "Total du nombre d'ETP",
                ROUND((etra."ETP Services généraux")/(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions"),2) as "Taux de personnel soins",
                ROUND((etra."ETP Socio-éducatif")/(etra."ETP Direction/Encadrement" + etra."ETP Administration /Gestion" + etra."ETP Services généraux" + etra."ETP Restauration" + etra."ETP Socio-éducatif" + etra."ETP Paramédical" + etra."ETP Psychologue" + etra."ETP ASH" + etra."ETP Médical" + etra."ETP Personnel Education nationale" + etra."ETP Autres fonctions"),2) as "Taux de personnel socio educatif",
                etra."Déficiences intellectuelles" AS "deficiences_intellectuelles",
                etra."Déficiences intellectuelles.1" AS "deficiences_intellectuelles1",
                etra."Déficiences auditives" AS "deficiences_auditives",
                etra."Déficiences auditives.1" AS "deficiences_auditives1",
                etra."Déficiences visuelles" AS "deficiences_visuelles",
                etra."Déficiences visuelles.1" AS "deficiences_visuelles1",
                etra."Déficiences motrices" AS "deficiences_motrices",
                etra."Déficiences motrices.1" AS "deficiences_motrices1",
                etra."Déficiences métaboliques, viscérales et nutritionnelles" AS "deficiences_metaboliques_viscerales_et_nutritionnelles",
                etra."Déficiences métaboliques, viscérales et nutritionnelles.1" AS "deficiences_metaboliques_viscerales_et_nutritionnelles1",
                etra."Autres types de déficiences" AS "autres_types_de_deficiences",
                etra."Autres types de déficiences.1" AS "autres_types_de_deficiences1",
                SUM(etra."% de personnes âgées de 20 - 29 ans"+
                etra."% de personnes âgées de 30 - 39 ans" +
                etra."% de personnes âgées de 40 - 49 ans"+
                etra."% de personnes âgées de 50 - 54 ans" +
                etra."% de personnes âgées de 55 - 59 ans" +
                etra."% de personnes âgées de 60 - 64 ans" +
                etra."% de personnes âgées de Plus de 65 ans") AS "Taux de résidents de plus de 20 ans" ,
                etra."Taux de vétusté des constructions" AS "Taux de vétusté des constructions",
                etra."Taux de vétusté des équipements en %" AS "Taux de vétusté des équipements en %",
                NULLTOZERO(rs.nb_recla) as "Nombre de réclamations sur la période """+param_N_3+"""-"""+param_N+""" ",
                NULLTOZERO(ROUND(CAST(rs.nb_recla AS FLOAT) / CAST(ccta.somme_de_capacite_autorisee_totale_ AS FLOAT), 4)*100) as "Rapport réclamations / capacité",
                NULLTOZERO(rs."Hôtellerie-locaux-restauration") as "Recla IGAS : Hôtellerie-locaux-restauration",
                NULLTOZERO(rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service") as "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
                NULLTOZERO(rs."Problème de qualité des soins médicaux") as "Recla IGAS : Problème de qualité des soins médicaux",
                NULLTOZERO(rs."Problème de qualité des soins paramédicaux") as "Recla IGAS : Problème de qualité des soins paramédicaux",
                NULLTOZERO(rs."Recherche d?établissement ou d?un professionnel") as "Recla IGAS : Recherche d’établissement ou d’un professionnel",
                NULLTOZERO(rs."Mise en cause attitude des professionnels") as "Recla IGAS : Mise en cause attitude des professionnels",
                NULLTOZERO(rs."Informations et droits des usagers") as "Recla IGAS : Informations et droits des usagers",
                NULLTOZERO(rs."Facturation et honoraires") as "Recla IGAS : Facturation et honoraires",
                NULLTOZERO(rs."Santé-environnementale") as "Recla IGAS : Santé-environnementale",
                NULLTOZERO(rs."Activités d?esthétique réglementées") as "Recla IGAS : Activités d’esthétique réglementées",
                NULLTOZERO(rs."Nombre d'EI sur la période 36mois") as "Nombre d'EI sur la période 36mois",
                NULLTOZERO(rs.NB_EIGS) as "Nombre d'EIG sur la période """+param_N_3+""" -"""+param_N+""" ",
                NULLTOZERO(rs.NB_EIAS) as "Nombre d'EIAS sur la période """+param_N_3+""" -"""+param_N+""" ",
                NULLTOZERO(rs."Nombre d'EI sur la période 36mois" + NULLTOZERO(rs.NB_EIGS) + NULLTOZERO(rs.NB_EIAS)) as "Somme EI + EIGS + EIAS sur la période """+param_N_4+""" -"""+param_N+"""",
                NULLTOZERO(rs."nb EI/EIG : Acte de prévention") as "nb EI/EIG : Acte de prévention",
                NULLTOZERO(rs."nb EI/EIG : Autre prise en charge") as "nb EI/EIG : Autre prise en charge",
                NULLTOZERO(rs."nb EI/EIG : Chute") as "nb EI/EIG : Chute",
                NULLTOZERO(rs."nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)") as "nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)",
                NULLTOZERO(rs."nb EI/EIG : Dispositif médical") as "nb EI/EIG : Dispositif médical",
                NULLTOZERO(rs."nb EI/EIG : Fausse route") as "nb EI/EIG : Fausse route",
                NULLTOZERO(rs."nb EI/EIG : Infection associée aux soins (IAS) hors ES") as "nb EI/EIG : Infection associée aux soins (IAS) hors ES",
                NULLTOZERO(rs."nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)") as "nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)",
                NULLTOZERO(rs."nb EI/EIG : Parcours/Coopération interprofessionnelle") as "nb EI/EIG : Parcours/Coopération interprofessionnelle",
                NULLTOZERO(rs."nb EI/EIG : Prise en charge chirurgicale") as "nb EI/EIG : Prise en charge chirurgicale",
                NULLTOZERO(rs."nb EI/EIG : Prise en charge diagnostique") as "nb EI/EIG : Prise en charge diagnostique",
                NULLTOZERO(rs."nb EI/EIG : Prise en charge en urgence") as "nb EI/EIG : Prise en charge en urgence",
                NULLTOZERO(rs."nb EI/EIG : Prise en charge médicamenteuse") as "nb EI/EIG : Prise en charge médicamenteuse",
                NULLTOZERO(rs."nb EI/EIG : Prise en charge des cancers") as "nb EI/EIG : Prise en charge des cancers",
                NULLTOZERO(rs."nb EI/EIG : Prise en charge psychiatrique") as "nb EI/EIG : Prise en charge psychiatrique",
                NULLTOZERO(rs."nb EI/EIG : Suicide") as "nb EI/EIG : Suicide",
                NULLTOZERO(rs."nb EI/EIG : Tentative de suicide") as "nb EI/EIG : Tentative de suicide",
            NULLTOZERO(i.'ICE """+param_N_1+""" (réalisé)') as 'ICE """+param_N+""" (réalisé)',
            NULLTOZERO(i.'Inspection SUR SITE """+param_N_1+"""- Déjà réalisée') as 'Inspection SUR SITE """+param_N+""" - Déjà réalisée',
            NULLTOZERO(i.'Controle SUR PIECE """+param_N_1+"""- Déjà réalisé') as 'Controle SUR PIECE """+param_N+""" - Déjà réalisé',
            NULLTOZERO(i.'Inspection / contrôle Programmé """+param_N+"""') as 'Inspection / contrôle Programmé """+param_N+"""'
                FROM tfiness_clean tf 
                LEFT JOIN finess_pivoted fp on fp.finess= tf.finess
                LEFT JOIN [errd_déficit_excédent_"""+param_N_4+"""] ede on tf.finess= SUBSTRING(ede."Structure - FINESS - RAISON SOCIALE", 1, 9)
                LEFT JOIN [errd_déficit_excédent_"""+param_N_3+"""] ede2 on  tf.finess = SUBSTRING(ede2."Structure - FINESS - RAISON SOCIALE", 1, 9)
                LEFT JOIN [errd_déficit_excédent_"""+param_N_2+"""] ede3 on tf.finess = SUBSTRING(ede3."Structure - FINESS - RAISON SOCIALE", 1, 9)
                LEFT JOIN communes c on c.com = tf.com_code
                LEFT JOIN departement_"""+param_N_2+"""  d on d.dep = c.dep
                LEFT JOIN region_"""+param_N_2+"""   r on d.reg = r.reg
                LEFT JOIN capacites_ehpad ce on ce."ET-N°FINESS" = tf.finess
                LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
                LEFT JOIN occupation_"""+param_N_5+"""_"""+param_N_4+""" o1 on o1.finess_19 = tf.finess
                LEFT JOIN occupation_"""+param_N_3+"""  o2  on o2.finess = tf.finess
                LEFT JOIN clean_occupation_N_2  co3  on co3.finess = tf.finess
                LEFT JOIN clean_tdb_n_2  etra on etra.finess = tf.finess
                LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
                LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
                LEFT JOIN EHPAD_Indicateurs_"""+param_N_3+"""_REG_agg eira on eira.et_finess = tf.finess
                LEFT JOIN clean_tdb_n_4  d2 on d2.finess = tf.finess
                LEFT JOIN clean_tdb_n_3  etra2 on etra2.finess = tf.finess
                LEFT JOIN recla_signalement rs on rs.finess = tf.finess
                LEFT JOIN inspections i on i.finess = tf.finess
                WHERE r.reg ='"""+str(region)+"""'
                GROUP by tf.finess """
            cursor.execute(df_controle)
            res=cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            df_controle=pd.DataFrame(res,columns=columns) 
    
    except Error as e:
        print(f"Erreur lors de l'exécution de la requête : {e}")
    finally:
        if conn:
            conn.close()
    
    date_string = datetime.today().strftime('%d%m%Y') 
    path = 'data/output/{}_{}.xlsx'.format(outputName(region),date_string)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(path, engine='xlsxwriter')
    df_ciblage.to_excel(writer, sheet_name='ciblage', index=False)
    df_controle.to_excel(writer, sheet_name='controle', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    print('export créé : {}_{}.xlsx'.format(outputName(region),date_string))
    return

     # Definitions des functions utiles en SQL
def nullToZero(value):
    if value is None:
        return 0
    else:
       return value
    
def moy3(value1, value2, value3):
    value_list = [value1,value2,value3]
    res = []
    for val in value_list:
        if val != None :
            res.append(str(val).replace(",", '.'))
    if len(res)== 0:
        return None
    else :
        clean_res = [float(i) for i in res]
        return sum(clean_res)/len(clean_res) #statistics.mean(res)



   