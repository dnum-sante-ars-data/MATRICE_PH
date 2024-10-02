import sqlite3
import json
import os
from os import listdir
import pandas as pd
from modules.init_db.init_db import conn_db
from utils import utils
from datetime import datetime
from sqlite3 import Error
import importlib.util

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

def init_table(conn):
    # Lire le nom de la base de données
    dbname = utils.read_settings("settings/settings.json", "db", "name")
    conn = conn_db(dbname)
    conn.create_function("NULLTOZERO",1, nullToZero)
    conn.create_function("MOY3", 3, moy3)
    cursor = conn.cursor()
    
    # Lire les paramètres depuis le fichier JSON
    with open('settings/settings.json') as f:
        data = json.load(f)

    # Vérifier si la clé 'parametres' existe dans le JSON
    if 'parametres' in data:
        params = data['parametres'][0]
        param_N = params['param_N']
        param_N_1 = params['param_N_1']
        param_N_2 = params['param_N_2']
        param_N_3 = params['param_N_3']
        param_N_4 = params['param_N_4']
        param_N_5 = params['param_N_5']
        param_fin_mois = params['param_fin_mois']
        param_debut_mois = params['param_debut_mois']
        param_debut_mois_N_3 = params['param_debut_mois_N_3']
    else:
        raise KeyError("La clé 'parametres' n'existe pas dans le fichier JSON")

    # Liste des tables à supprimer
    tables_to_drop = [
        "tfiness_clean", "finess_pivoted","table_recla", "igas", "table_signalement", "sign", "sign_HDF",
        "recla_signalement", "recla_signalement_HDF", "clean_occupation_N_2", 
        "clean_capacite_totale_auto", "clean_hebergement", "clean_tdb_n_4", 
        "clean_tdb_n_3", "clean_tdb_n_2", "correspondance", "grouped_errd_charges", 
        "grouped_errd_produitstarif", "grouped_errd_produits70", 
        "grouped_errd_produitsencaiss", "grouped_caph_charges", 
        "grouped_caph_produitstarif", "grouped_caph_produits70", 
        "grouped_caph_produitsencaiss", "grouped_capa_charges", 
        "grouped_capa_produitstarif", "charges_produits", "inspections", "communes", "moyenne_ciblage", "moyenne_controle"
    ]

    # Supprimer les tables existantes
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
        print(f"Table {table} supprimée.")

    # Dossier contenant les fichiers de requêtes SQL
    sql_directory = utils.read_settings("settings/settings.json", "path", "sql_directory")

    # Vérifiez si le dossier existe
    if not os.path.exists(sql_directory):
        raise FileNotFoundError(f"Le dossier spécifié n'existe pas : {sql_directory}")

    # Boucle à travers chaque fichier Python dans le dossier
    for filename in os.listdir(sql_directory):
        if filename.endswith('.py'):
            file_path = os.path.join(sql_directory, filename)

            # Charger et exécuter le fichier Python
            try:
                # Charger le module
                spec = importlib.util.spec_from_file_location(filename[:-3], file_path)  # sans l'extension .py
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # Vérifier si une variable sql_query existe dans le module
                if hasattr(module, 'sql_query'):
                    sql_query = module.sql_query

                    # Remplacer les paramètres dans la requête
                    sql_query = sql_query.format(
                        param_N=param_N,
                        param_N_1=param_N_1,
                        param_N_2=param_N_2,
                        param_N_3=param_N_3,
                        param_N_4=param_N_4,
                        param_N_5=param_N_5,
                        param_fin_mois=param_fin_mois,
                        param_debut_mois=param_debut_mois,
                        param_debut_mois_N_3=param_debut_mois_N_3
                    )

                    # Exécuter la requête SQL
                    cursor.execute(sql_query)  # Exécuter la requête SQL
                    print(f"Requête exécutée avec succès : {filename}")
                else:
                    print(f"Aucune requête SQL trouvée dans {filename}.")
            except Exception as e:
                print(f"Erreur lors de l'exécution de {filename}: {e}")

    # Commit après toutes les requêtes
    conn.commit()
    conn.close()  # Fermer la connexion


# Fonction principale pour exécuter les transformations
def execute_transform(region):
    dbname = utils.read_settings("settings/settings.json", "db", "name")
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
        ROUND(CAST(REPLACE(eira."Taux_plus_10_médics (cip13)", ",", ".") AS FLOAT),2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
        --ROUND(CAST(REPLACE(eira.taux_atu, ",", ".") AS FLOAT),2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
        "" as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
        CAST(REPLACE(taux_hospit_mco, ",", ".") AS FLOAT) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
        CAST(REPLACE(taux_hospit_had, ",", ".") AS FLOAT) as "Taux de recours à l'HAD des résidents d'EHPAD",
        "deficit ou excédent" AS "deficit ou excédent """+param_N_4+""" ",
        "deficit ou excédent" AS "deficit ou excédent """+param_N_3+""" ",
        "deficit ou excédent" AS "deficit ou excédent """+param_N_2+""" ",	     
        "" as "Saisie des indicateurs du TDB MS (campagne """+param_N_2+""")",
        CAST(d2."Taux d'absentéisme (hors formation) en %" as decimal) as "Taux d'absentéisme """+param_N_4+"""",
        etra2."Taux d'absentéisme (hors formation) en %"    as "Taux d'absentéisme """+param_N_4+"'"""",
        CAST(etra."Taux d'absentéisme (hors formation) en %" as FLOAT) as "Taux d'absentéisme """+param_N_2+"""",
        ROUND(MOY3(d2."Taux d'absentéisme (hors formation) en %" ,etra2."Taux d'absentéisme (hors formation) en %", etra."Taux d'absentéisme (hors formation) en %") ,2) as "Absentéisme moyen sur la période """+param_N_4+"""-"""+param_N_2+"""",
        CAST(d2."Taux de rotation des personnels" as decimal) as "Taux de rotation du personnel titulaire """+param_N_4+"""",
        etra2."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_3+"""",
        CAST(etra."Taux de rotation des personnels" as FLOAT) as "Taux de rotation du personnel titulaire """+param_N_2+"""",
        ROUND(MOY3(d2."Taux de rotation des personnels" , etra2."Taux de rotation des personnels" , CAST(etra."Taux de rotation des personnels" as FLOAT)), 2) as "Rotation moyenne du personnel sur la période """+param_N_4+"""-"""+param_N_2+"""",
        CAST(d2."Taux d'ETP vacants" as decimal) as "ETP vacants """+param_N_4+"""",
        etra2."Taux d'ETP vacants"  as "ETP vacants """+param_N_3+"""",
        CAST(etra."Taux d'ETP vacants" as FLOAT) as "ETP vacants """+param_N_2+"""",
        etra."Dont taux d'ETP vacants concernant la fonction SOINS" as "dont fonctions soins """+param_N_2+"""",
        etra."Dont taux d'ETP vacants concernant la fonction SOCIO EDUCATIVE"as "dont fonctions socio-éducatives """+param_N_2+"""", 
        CAST(REPLACE(d2."Taux de prestations externes sur les prestations directes",',','.')as decimal) as "Taux de prestations externes sur les prestations directes """+param_N_4+"""",
        etra2."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes """+param_N_3+"""", 
        CAST(etra."Taux de prestations externes sur les prestations directes" as FLOAT) as "Taux de prestations externes sur les prestations directes """+param_N_2+"""",
        ROUND(MOY3(d2."Taux de prestations externes sur les prestations directes" , etra2."Taux de prestations externes sur les prestations directes" , CAST(etra."Taux de prestations externes sur les prestations directes" as FLOAT)) ,4) as "Taux moyen de prestations externes sur les prestations directes",
        ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_4+"""",
        ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) as "Nombre total d'ETP par usager en """+param_N_3+"""",
        ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_2+"""",
        MOY3(ROUND(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) , ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) , 
        ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2),2))AS "Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+"""",
        ROUND((cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Médical" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "ETP 'soins' par usager en """+param_N_2+"""",
        ROUND(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT), 2) as "Total du nombre d'ETP",
        ROUND((cAST(etra."ETP Services généraux" as FLOAT))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2) as "Taux de personnel soins",
        ROUND((cAST(etra."ETP Socio-éducatif" as float))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2) as "Taux de personnel socio educatif",
        cAST(etra."Déficiences intellectuelles" as FLOAT) AS "deficiences_intellectuelles",
        cAST(etra."Déficiences intellectuelles.1" as FLOAT) AS "deficiences_intellectuelles1",
        cAST(etra."Déficiences auditives" as FLOAT) AS "deficiences_auditives",
        cAST(etra."Déficiences auditives.1" as FLOAT) AS "deficiences_auditives1",
        cAST(etra."Déficiences visuelles" as FLOAT) AS "deficiences_visuelles",
        cAST(etra."Déficiences visuelles.1" as FLOAT) AS "deficiences_visuelles1",
        cAST(etra."Déficiences motrices" as FLOAT) AS "deficiences_motrices",
        cAST(etra."Déficiences motrices.1" as FLOAT) AS "deficiences_motrices1",
        cAST(etra."Déficiences métaboliques, viscérales et nutritionnelles" as FLOAT) AS "deficiences_metaboliques_viscerales_et_nutritionnelles",
        cAST(etra."Déficiences métaboliques, viscérales et nutritionnelles.1" as FLOAT) AS "deficiences_metaboliques_viscerales_et_nutritionnelles1",
        cAST(etra."Autres types de déficiences" as FLOAT) AS "autres_types_de_deficiences",
        cAST(etra."Autres types de déficiences.1" as FLOAT) AS "autres_types_de_deficiences1",
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
        NULLTOZERO(i.'Inspection SUR SITE """+param_N_1+""" - Déjà réalisée') as 'Inspection SUR SITE """+param_N+""" - Déjà réalisée',
        NULLTOZERO(i.'Controle SUR PIECE """+param_N_1+""" - Déjà réalisé') as 'Controle SUR PIECE """+param_N+""" - Déjà réalisé',
        NULLTOZERO(i.'Inspection / contrôle Programmé """+param_N+"""') as 'Inspection / contrôle Programmé """+param_N+"""'
        FROM
        tfiness_clean tf 
        LEFT JOIN finess_pivoted fp on fp.finess=tf.finess
        LEFT JOIN errd_déficit_excédent_"""+param_N_4+""" on tf.finess= ede."FINESS de rattachement"
        LEFT JOIN errd_déficit_excédent_"""+param_N_3+""" on  tf.finess = ede2."FINESS de rattachement"
        LEFT JOIN errd_déficit_excédent_"""+param_N_2+""" on tf.finess = ede3."FINESS de rattachement"
        LEFT JOIN communes c on c.com = tf.com_code
        LEFT JOIN departement_"""+param_N+""" d on d.dep = c.dep
        LEFT JOIN region_"""+param_N+"""  r on d.reg = r.reg
        LEFT JOIN capacites_ehpad ce on ce."ET-N°FINESS" = tf.finess
        LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
        LEFT JOIN occupation_"""+param_N_5+"""_"""+param_N_4+""" o1 on o1.finess_19 = tf.finess
        LEFT JOIN occupation_"""+param_N_3+""" o2  on o2.finess = tf.finess
        LEFT JOIN clean_occupation_N_2 co3  on co3.finess = tf.finess
        LEFT JOIN clean_tdb_n_2 etra on etra.finess = tf.finess
        LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
        LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
        LEFT JOIN EHPAD_Indicateurs_"""+param_N_2+"""_REG_agg eira on eira."et finess" = tf.finess
        LEFT JOIN clean_tdb_n_4 d2 on SUBSTRING(d2.finess,1,9) = tf.finess
        LEFT JOIN clean_tdb_n_3 etra2 on etra2.finess = tf.finess
        LEFT JOIN clean_tdb_n_4 d2 on SUBSTRING(d2.finess,1,9) = tf.finess
        LEFT JOIN recla_signalement_HDF rs on rs.finess = tf.finess
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
        ROUND(CAST(REPLACE(eira."Taux_plus_10_médics (cip13)", ",", ".") AS FLOAT),2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
        --ROUND(CAST(REPLACE(eira.taux_atu, ",", ".") AS FLOAT),2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
        "" as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
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
        ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_2+"""",
        MOY3(ROUND(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) , ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) , ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2),2))AS "Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+"""",
        ROUND((cAST(etra."ETP Paramédical" as FLOAT) + etra."ETP Médical")/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "ETP 'soins' par usager en """+param_N_2+"""",
        cAST(etra."ETP Direction/Encadrement" as FLOAT) AS "Direction / Encadrement",
        etra."- Dont nombre d'ETP réels de personnel médical d'encadrement" AS "dont personnel médical d'encadrement",
        etra."_dont_autre_directionencadrement" AS "dont autre Direction / Encadrement",
        cAST(etra."ETP Administration /Gestion" as FLOAT) AS "Administration / Gestion",
        cAST(etra."ETP Services généraux" as FLOAT) AS "Services généraux",
        cAST(etra."ETP Restauration" as FLOAT) AS "Restauration",
        cAST(etra."ETP Socio-éducatif" as float) AS "Socio-éducatif",
        etra."- Dont nombre d'ETP réels d'aide médico-psychologique" AS "dont AMP",
        etra."- Dont nombre d'ETP réels d'animateur" AS "dont animateur",
        etra."- Dont nombre d'ETP réels de moniteur éducateur au 31.12" AS "dont moniteur éducateur",
        etra."- Dont nombre d’ETP réels d’éducateur spécialisé au 31.12" AS "dont éducateur spécialisé",
        etra."- Dont nombre d’ETP réels d’assistant social au 31.12" AS "dont assistant(e) social(e)",
        etra."-_dont_autre_socio-educatif" AS "dont autre socio-éducatif",
        cAST(etra."ETP Paramédical" as FLOAT) AS "Paramédical",
        etra."- Dont nombre d'ETP réels d'infirmier" AS "dont infirmier",
        etra."- Dont nombre d'ETP réels d'aide médico-psychologique.1" AS "dont AMP",
        etra."- Dont nombre d'ETP réels d'aide soignant" AS "dont aide-soignant(e) ",
        etra."- Dont nombre d'ETP réels de kinésithérapeute" AS "dont kinésithérapeute",
        etra."- Dont nombre d'ETP réels de psychomotricien" AS "dont psychomotricien(ne)",
        etra."- Dont nombre d'ETP réels d'ergothérapeute" AS "dont ergothérapeute",
        etra."- Dont nombre d'ETP réels d'orthophoniste" AS "dont orthophoniste",
        etra."-_dont_autre_paramedical" AS "dont autre paramédical",
        cAST(etra."ETP Psychologue" as FLOAT) AS "Psychologue",
        cAST(etra."ETP ASH" as FLOAT) AS "ASH",
        cAST(etra."ETP Médical" as FLOAT) AS "Médical",
        cast(etra."- Dont nombre d'ETP réels de médecin coordonnateur" as FLOAT) as "dont médecin coordonnateur",
        etra."-_dont_autre_medical" AS "dont autre médical",
        cAST(etra."ETP Personnel Education nationale" as FLOAT) AS "Personnel éducation nationale",
        cAST(etra."ETP Autres fonctions" as FLOAT) AS "Autres fonctions",
        ROUND(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT), 2) as "Total du nombre d'ETP",
        ROUND((cAST(etra."ETP Services généraux" as FLOAT))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2) as "Taux de personnel soins",
        ROUND((cAST(etra."ETP Socio-éducatif" as float))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2) as "Taux de personnel socio educatif",
        cAST(etra."Déficiences intellectuelles" as FLOAT) AS "deficiences_intellectuelles",
        cAST(etra."Déficiences intellectuelles.1" as FLOAT) AS "deficiences_intellectuelles1",
        cAST(etra."Déficiences auditives" as FLOAT) AS "deficiences_auditives",
        cAST(etra."Déficiences auditives.1" as FLOAT) AS "deficiences_auditives1",
        cAST(etra."Déficiences visuelles" as FLOAT) AS "deficiences_visuelles",
        cAST(etra."Déficiences visuelles.1" as FLOAT) AS "deficiences_visuelles1",
        cAST(etra."Déficiences motrices" as FLOAT) AS "deficiences_motrices",
        cAST(etra."Déficiences motrices.1"as FLOAT) AS "deficiences_motrices1",
        cAST(etra."Déficiences métaboliques, viscérales et nutritionnelles" as FLOAT) AS "deficiences_metaboliques_viscerales_et_nutritionnelles",
        cAST(etra."Déficiences métaboliques, viscérales et nutritionnelles.1" as FLOAT) AS "deficiences_metaboliques_viscerales_et_nutritionnelles1",
        cAST(etra."Autres types de déficiences" as FLOAT) AS "autres_types_de_deficiences",
        cAST(etra."Autres types de déficiences.1" as FLOAT) AS "autres_types_de_deficiences1",
        SUM(etra."_de_personnes_agees_de_20_-_29_ans" +
        etra."_de_personnes_agees_de_30_-_39_ans" +
        etra."_de_personnes_agees_de_40_-_49_ans" +
        etra."_de_personnes_agees_de_50_-_54_ans" +
        etra."_de_personnes_agees_de_55_-_59_ans" +
        etra."_de_personnes_agees_de_60_-_64_ans" +
        etra."_de_personnes_agees_de_plus_de_65_ans") AS Taux de résidents de plus de 20 ans , 
        cAST(etra."Taux de vétusté des constructions" as FLOAT) AS Taux de vétusté des constructions,
        cAST(etra."Taux de vétusté des équipements en %" as FLOAT) AS "Taux de vétusté des équipements en %",
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
        NULLTOZERO(i.'Inspection SUR SITE """+param_N_1+""" - Déjà réalisée') as 'Inspection SUR SITE """+param_N+""" - Déjà réalisée',
        NULLTOZERO(i.'Controle SUR PIECE """+param_N_1+""" - Déjà réalisé') as 'Controle SUR PIECE """+param_N+""" - Déjà réalisé',
        NULLTOZERO(i.'Inspection / contrôle Programmé """+param_N+"""') as 'Inspection / contrôle Programmé """+param_N+"""'
        FROM
        tfiness_clean tf 
        LEFT JOIN finess_pivoted fp on fp.finess=tf.finess
        LEFT JOIN [errd_déficit_excédent_"""+param_N_4+"""] ede on tf.finess= SUBSTRING(ede."Structure - FINESS - RAISON SOCIALE", 1, 9)
        LEFT JOIN [errd_déficit_excédent_"""+param_N_3+"""] ede2 on  tf.finess = SUBSTRING(ede2."Structure - FINESS - RAISON SOCIALE", 1, 9)
        LEFT JOIN [errd_déficit_excédent_"""+param_N_2+"""] ede3 on tf.finess = SUBSTRING(ede3."Structure - FINESS - RAISON SOCIALE", 1, 9)
        LEFT JOIN communes c on c.com = tf.com_code
        LEFT JOIN departement_"""+param_N+""" d on d.dep = c.dep
        LEFT JOIN region_"""+param_N+"""  r on d.reg = r.reg
        LEFT JOIN capacites_ehpad ce on ce."ET-N°FINESS" = tf.finess
        LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
        LEFT JOIN occupation_"""+param_N_5+"""_"""+param_N_4+""" o1 on o1.finess_19 = tf.finess
        LEFT JOIN occupation_"""+param_N_3+""" o2  on o2.finess = tf.finess
        LEFT JOIN clean_occupation_N_2 co3  on co3.finess = tf.finess
        LEFT JOIN clean_tdb_n_2 etra on etra.finess = tf.finess
        LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
        LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
        LEFT JOIN EHPAD_Indicateurs_"""+param_N_2+"""_REG_agg eira on eira."et finess" = tf.finess
        LEFT JOIN clean_tdb_n_4 d2 on SUBSTRING(d2.finess,1,9) = tf.finess
        LEFT JOIN clean_tdb_n_3 etra2 on etra2.finess = tf.finess
        LEFT JOIN clean_tdb_n_4 d2 on SUBSTRING(d2.finess,1,9) = tf.finess
        LEFT JOIN recla_signalement_HDF rs on rs.finess = tf.finess
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
        ROUND(CAST(REPLACE(eira."Taux_plus_10_médics (cip13)", ",", ".") AS FLOAT),2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
        --ROUND(CAST(REPLACE(eira.taux_atu, ",", ".") AS FLOAT),2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
        "" as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
        ROUND(CAST(REPLACE(taux_hospit_mco, ",", ".") AS FLOAT),2) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
        ROUND(CAST(REPLACE(taux_hospit_had, ",", ".") AS FLOAT),2) as "Taux de recours à l'HAD des résidents d'EHPAD",
        ede."deficit ou excédent" AS "deficit ou excédent """+param_N_4+""" ",
        ede2."deficit ou excédent" AS "deficit ou excédent """+param_N_3+""" ",
        ede3."deficit ou excédent" AS "deficit ou excédent """+param_N_2+""" ",
        "" as "Saisie des indicateurs du TDB MS (campagne """+param_N_2+""")",
        CAST(d2."Taux d'absentéisme (hors formation) en %" as decimal) as "Taux d'absentéisme """+param_N_4+"""",
        etra2."Taux d'absentéisme (hors formation) en %"     as "Taux d'absentéisme """+param_N_3+"""",
        CAST(etra."Taux d'absentéisme (hors formation) en %" as Float) as "Taux d'absentéisme """+param_N_2+"""",
        ROUND(MOY3(d2."Taux d'absentéisme (hors formation) en %" ,etra2."Taux d'absentéisme (hors formation) en %"    , etra."Taux d'absentéisme (hors formation) en %") ,2) as "Absentéisme moyen sur la période """+param_N_4+"""-"""+param_N_2+"""",
        CAST(d2."Taux de rotation des personnels" as decimal) as "Taux de rotation du personnel titulaire """+param_N_4+"""",
        etra2."Taux de rotation des personnels" as "Taux de rotation du personnel titulaire """+param_N_3+"""",
        CAST(etra."Taux de rotation des personnels" as FLOAT) as "Taux de rotation du personnel titulaire """+param_N_2+"""",
        ROUND(MOY3(d2."Taux de rotation des personnels" , etra2."Taux de rotation des personnels" , CAST(etra."Taux de rotation des personnels" as FLOAT)), 2) as "Rotation moyenne du personnel sur la période """+param_N_4+"""-"""+param_N_2+"""",
        CAST(d2."Taux d'ETP vacants" as decimal) as "ETP vacants """+param_N_4+"""",
        CAST(etra2."Taux d'ETP vacants" as decimal)  as "ETP vacants """+param_N_3+"""",
        CAST(etra."Taux d'ETP vacants" as decimal) as "ETP vacants """+param_N_2+"""",
        CAST(etra."Dont taux d'ETP vacants concernant la fonction SOINS" as decimal) as "dont fonctions soins """+param_N_2+"""",
        CAST(etra."Dont taux d'ETP vacants concernant la fonction SOCIO EDUCATIVE" as decimal) as "dont fonctions socio-éducatives """+param_N_2+"""", 
        CAST(REPLACE(d2."Taux de prestations externes sur les prestations directes",',','.')as decimal) as "Taux de prestations externes sur les prestations directes """+param_N_4+"""",
        etra2."Taux de prestations externes sur les prestations directes" as "Taux de prestations externes sur les prestations directes """+param_N_3+"""", 
        Cast(etra."Taux de prestations externes sur les prestations directes" as decimal) as "Taux de prestations externes sur les prestations directes"""+param_N_2+"""",
        ROUND(MOY3(d2."Taux de prestations externes sur les prestations directes" , etra2."Taux de prestations externes sur les prestations directes" , CAST(etra."Taux de prestations externes sur les prestations directes" as float)) ,2) as "Taux moyen de prestations externes sur les prestations directes",
        ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_4+"""",
        ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) as "Nombre total d'ETP par usager en """+param_N_3+"""",
        ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_2+"""",
        ROUND(MOY3(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) , ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) , 
        ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)),2)AS "Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+"""",
        ROUND((cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Médical" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "ETP 'soins' par usager en """+param_N_2+"""",
        cast(etra."- Dont nombre d'ETP réels de médecin coordonnateur" as FLOAT) as "dont médecin coordonnateur",
        ROUND(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT), 2) as "Total du nombre d'ETP",
        ROUND((cAST(etra."ETP Services généraux" as FLOAT))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2) as "Taux de personnel soins",
        ROUND((cAST(etra."ETP Socio-éducatif" as float))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2) as "Taux de personnel socio educatif",
        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_intellectuelles_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Déficiences intellectuelles principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Autisme_et_autres_TED_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Autisme et autres TED principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Troubles_du_psychisme_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Troubles du psychisme principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Troubles_du_langage_et_des_apprentissages_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Troubles du langage et des apprentissages principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_auditives_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Déficiences auditives principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_visuelles_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Déficiences visuelles principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_motrices_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Déficiences motrices principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_métaboliques_viscérales_et_nutritionnelles_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Déficiences métaboliques viscérales et nutritionnelles principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Cérébro_lésions_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Cérébro lésions principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Polyhandicap_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Polyhandicap principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Troubles_du_comportement_et_de_la_communication_princiaples(TCC)" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Troubles du comportement et de la communication principales (TCC)",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Diagnostics_en_cours_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Diagnostics en cours principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Autres_types_de_déficiences_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Autres types de déficiences principales",
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
        NULLTOZERO(rs."Nombre d'EI sur la période 36mois") + NULLTOZERO(rs.NB_EIGS) + NULLTOZERO(rs.NB_EIAS) as "Somme EI + EIGS + EIAS sur la période """+param_N_3+"""-"""+param_N+"""",
        NULLTOZERO(i.'ICE """+param_N_1+""" (réalisé)') as 'ICE """+param_N+""" (réalisé)',
        NULLTOZERO(i.'Inspection SUR SITE """+param_N_1+""" - Déjà réalisée') as 'Inspection SUR SITE """+param_N+""" - Déjà réalisée',
        NULLTOZERO(i.'Controle SUR PIECE """+param_N_1+""" - Déjà réalisé') as 'Controle SUR PIECE """+param_N+""" - Déjà réalisé',
        NULLTOZERO(i.'Inspection / contrôle Programmé """+param_N+"""') as 'Inspection / contrôle Programmé """+param_N+"""',
        MAX(CAST(SUBSTR(hsm."Date réelle Visite", 7, 4) as INTEGER)) as "Année dernière inspection (sur pl ou sur pi)"
        FROM tfiness_clean tf 
        LEFT JOIN finess_pivoted fp on fp.finess= tf.finess
        LEFT JOIN [errd_déficit_excédent_"""+param_N_4+"""] ede on tf.finess= SUBSTRING(ede."Structure - FINESS - RAISON SOCIALE", 1, 9)
        LEFT JOIN [errd_déficit_excédent_"""+param_N_3+"""] ede2 on  tf.finess = SUBSTRING(ede2."Structure - FINESS - RAISON SOCIALE", 1, 9)
        LEFT JOIN [errd_déficit_excédent_"""+param_N_2+"""] ede3 on tf.finess = SUBSTRING(ede3."Structure - FINESS - RAISON SOCIALE", 1, 9)
        LEFT JOIN communes c on c.com = tf.com_code
        LEFT JOIN departement_"""+param_N+""" d on d.dep = c.dep
        LEFT JOIN region_"""+param_N+""" r on d.reg = r.reg
        LEFT JOIN capacites_ehpad ce on ce."ET-N°FINESS" = tf.finess
        LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
        LEFT JOIN occupation_"""+param_N_5+"""_"""+param_N_4+"""  o1 on o1.finess_19 = tf.finess
        LEFT JOIN occupation_"""+param_N_3+"""  o2  on o2.finess = tf.finess
        LEFT JOIN clean_occupation_N_2  co3  on co3.finess = tf.finess
        LEFT JOIN clean_tdb_n_2  etra on etra.finess = tf.finess
        LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
        LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
        LEFT JOIN EHPAD_Indicateurs_"""+param_N_2+"""_REG_agg eira on eira."et finess" = tf.finess
        LEFT JOIN clean_tdb_n_4  d2 on d2.finess = tf.finess
        LEFT JOIN clean_tdb_n_3  etra2 on etra2.finess = tf.finess
        LEFT JOIN recla_signalement rs on rs.finess = tf.finess
        LEFT JOIN inspections i on i.finess = tf.finess
        LEFT JOIN HELIOS_SICEA_MISSIONS_"""+param_N+""" hsm on IIF(LENGTH("Code FINESS") = 8, '0' || "Code FINESS", "Code FINESS") = tf.finess
        WHERE r.reg ='"""+str(region)+"""'
        GROUP BY tf.finess 
        UNION
        SELECT 
        "" as "","" as "","" AS "",""as "","" as "","" as "","" as "","" as "","" as "","" as "","" AS "","" AS "",
        "" AS "Moyenne nationale", 
        mcb."Capacité totale autorisée",
        mcb."Type d'activité indifferencié",
        mcb."Protection Juridique",
        mcb."Hébergement Complet Internat",
        mcb."Placement Famille d'Accueil",
        mcb."Accueil temporaire avec hébergement",
        mcb."Prestation en milieu ordinaire",
        mcb."Accueil de jour et accompagnement en milieu ordinaire",
        mcb."Accueil de Jour",
        mcb."Accueil temporaire (avec et sans hébergement)",
        mcb."Semi-Internat",
        mcb."Hébergement de Nuit Eclaté",
        mcb."Internat de Semaine",
        mcb."Externat",
        mcb."Tous modes d'accueil et d'accompagnement",
        mcb."Tous modes d'accueil avec hébergement",
        mcb."Accueil de Nuit",
        mcb."Permanence téléphonique",
        mcb."Accueil temporaire de jour",
        mcb."Consultation Soins Externes",
        mcb."Accueil modulable/séquentiel",
        mcb."Traitement et Cure Ambulatoire",
        mcb."Tous modes d'accueil (avec et sans hébergement)",
        mcb."Equipe mobile de rue",
        mcb."Accueil et prise en charge en appartement thérapeutique",
        mcb."Hospitalisation Complète",
        mcb."Regroupement Calcules (Annexes XXIV)",
        mcb."Aide Judiciaire à la Gestion du Budget Familial",
        mcb."Accompagnement Social Personnalisé",
        mcb."Information des Tuteurs Familiaux",
        mcb."Administration",
        mcb."Nombre de résidents au 31/12/"""+param_N_2+"""",
        mcb."Nombre de places installées au 31/12/"""+param_N_2+"""",
        mcb."Part des résidents ayant plus de 10 médicaments consommés par mois",
        mcb."Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
        mcb."Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
        mcb."Taux de recours à l'HAD des résidents d'EHPAD",
        mcb."deficit ou excédent """+param_N_4+"""",
        mcb."deficit ou excédent """+param_N_3+"""",
        mcb."deficit ou excédent """+param_N_2+"""",
        mcb."Saisie des indicateurs du TDB MS (campagne """+param_N_2+""")",
        mcb."Taux d'absentéisme """+param_N_4+"""",
        mcb."Taux d'absentéisme """+param_N_3+"""",
        mcb."Taux d'absentéisme """+param_N_2+"""",
        mcb."Absentéisme moyen sur la période """+param_N_4+"""-"""+param_N_2+"""",
        mcb."Taux de rotation du personnel titulaire """+param_N_4+"""",
        mcb."Taux de rotation du personnel titulaire """+param_N_3+"""",
        mcb."Taux de rotation du personnel titulaire """+param_N_2+"""",
        mcb."Rotation moyenne du personnel sur la période """+param_N_4+"""-"""+param_N_2+"""",
        mcb."ETP vacants """+param_N_4+"""",
        mcb."ETP vacants """+param_N_3+"""",
        mcb."ETP vacants """+param_N_2+"""",
        mcb."dont fonctions soins """+param_N_2+"""",
        mcb."dont fonctions socio-éducatives """+param_N_2+"""",
        mcb."Taux de prestations externes sur les prestations directes """+param_N_4+"""",
        mcb."Taux de prestations externes sur les prestations directes """+param_N_3+"""",
        mcb."Taux de prestations externes sur les prestations directes """+param_N_2+"""",
        mcb."Taux moyen de prestations externes sur les prestations directes",
        mcb."Nombre total d'ETP par usager en """+param_N_4+"""",
        mcb."Nombre total d'ETP par usager en """+param_N_3+"""",
        mcb."Nombre total d'ETP par usager en """+param_N_2+"""",
        mcb."Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+"""",
        mcb."ETP 'soins' par usager en """+param_N_2+"""",
        mcb."dont médecin coordonnateur",
        mcb."Total du nombre d'ETP",
        mcb."Taux de personnel soins",
        mcb."Taux de personnel socio educatif",
        mcb."Taux de résidents par Déficiences intellectuelles principales",
        mcb."Taux de résidents par Autisme et autres TED principales" ,
        mcb."Taux de résidents par Troubles du psychisme principales",
        mcb."Taux de résidents par Troubles du langage et des apprentissages principales",
        mcb."Taux de résidents par Déficiences auditives principales",
        mcb."Taux de résidents par Déficiences visuelles principales",
        mcb."Taux de résidents par Déficiences motrices principales",
        mcb."Taux de résidents par Déficiences métaboliques viscérales et nutritionnelles principales",
        mcb."Taux de résidents par Cérébro lésions principales",
        mcb."Taux de résidents par Polyhandicap principales",
        mcb."Taux de résidents par Troubles du comportement et de la communication principales (TCC)",
        mcb."Taux de résidents par Diagnostics en cours principales",
        mcb."Taux de résidents par Autres types de déficiences principales",
        mcb."Nombre de réclamations sur la période """+param_N_3+"""-"""+param_N+"""",
        mcb."Rapport réclamations / capacité",
        mcb."Recla IGAS : Hôtellerie-locaux-restauration",
        mcb."Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
        mcb."Recla IGAS : Problème de qualité des soins médicaux",
        mcb."Recla IGAS : Problème de qualité des soins paramédicaux",
        mcb."Recla IGAS : Recherche d’établissement ou d’un professionnel",
        mcb."Recla IGAS : Mise en cause attitude des professionnels",
        mcb."Recla IGAS : Informations et droits des usagers",
        mcb."Recla IGAS : Facturation et honoraires",
        mcb."Recla IGAS : Santé environnementale",
        mcb."Recla IGAS : Activités d’esthétique réglementées",
        mcb."Nombre d'EIG sur la période """+param_N_3+"""-"""+param_N+"""",
        mcb."Nombre d'EIAS sur la période """+param_N_3+"""-"""+param_N+"""",
        mcb."Somme EI + EIGS + EIAS sur la période """+param_N_3+"""-"""+param_N_1+"""",
        mcb."ICE """+param_N+""" (réalisé)",
        mcb."Inspection SUR SITE """+param_N+""" - Déjà réalisée",
        mcb."Controle SUR PIECE """+param_N+""" - Déjà réalisé",
        mcb."Inspection / contrôle Programmé """+param_N+"""",
        mcb."Année dernière inspection (sur pl ou sur pi)"
        From moyenne_ciblage mcb
        """
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
    ROUND(CAST(REPLACE(eira."Taux_plus_10_médics (cip13)", ",", ".") AS FLOAT),2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
    --ROUND(CAST(REPLACE(eira.taux_atu, ",", ".") AS FLOAT),2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
    "" as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
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
    ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "Nombre total d'ETP par usager en """+param_N_2+""" ",
    ROUND(MOY3(ROUND((d2."ETP Direction/Encadrement" + d2."ETP Administration /Gestion" + d2."ETP Services généraux" + d2."ETP Restauration" + d2."ETP Socio-éducatif" + d2."ETP Paramédical" + d2."ETP Psychologue" + d2."ETP ASH" + d2."ETP Médical" + d2."ETP Personnel Education nationale" + d2."ETP Autres fonctions")/d2."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) , ROUND((etra2."ETP Direction/Encadrement" + etra2."ETP Administration /Gestion" + etra2."ETP Services généraux" + etra2."ETP Restauration" + etra2."ETP Socio-éducatif" + etra2."ETP Paramédical" + etra2."ETP Psychologue" + etra2."ETP ASH" + etra2."ETP Médical" + etra2."ETP Personnel Education nationale"  + etra2."ETP Autres fonctions" )/etra2."Nombre de personnes accompagnées dans l'effectif au 31.12" , 2) , ROUND((cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)),2)AS "Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+""" ",
    ROUND((cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Médical" as FLOAT))/etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2) as "ETP 'soins' par usager en """+param_N_2+""" ",
    cAST(etra."ETP Direction/Encadrement" as FLOAT) AS "Direction / Encadrement",
    etra."- Dont nombre d'ETP réels de personnel médical d'encadrement" AS "dont personnel médical d'encadrement",
    etra."_dont_autre_directionencadrement" AS "dont autre Direction / Encadrement",
    cAST(etra."ETP Administration /Gestion" as FLOAT) AS "Administration / Gestion",
    cAST(etra."ETP Services généraux" as FLOAT) AS "Services généraux",
    cAST(etra."ETP Restauration" as FLOAT) AS "Restauration",
    cAST(etra."ETP Socio-éducatif" as float) AS "Socio-éducatif",
    etra."- Dont nombre d'ETP réels d'aide médico-psychologique" AS "dont AMP",
    etra."- Dont nombre d'ETP réels d'animateur" AS "dont animateur",
    etra."- Dont nombre d'ETP réels de moniteur éducateur au 31.12" AS "dont moniteur éducateur",
    etra."- Dont nombre d’ETP réels d’éducateur spécialisé au 31.12" AS "dont éducateur spécialisé",
    etra."- Dont nombre d’ETP réels d’assistant social au 31.12" AS "dont assistant(e) social(e)",
    etra."-_dont_autre_socio-educatif" AS "dont autre socio-éducatif",
    cAST(etra."ETP Paramédical" as FLOAT) AS "Paramédical",
    etra."- Dont nombre d'ETP réels d'infirmier" AS "dont infirmier",
    etra."- Dont nombre d'ETP réels d'aide médico-psychologique.1" AS "dont AES",
    etra."- Dont nombre d'ETP réels d'aide soignant" AS "dont aide-soignant(e) ",
    etra."- Dont nombre d'ETP réels de kinésithérapeute" AS "dont kinésithérapeute",
    etra."- Dont nombre d'ETP réels de psychomotricien" AS "dont psychomotricien(ne)",
    etra."- Dont nombre d'ETP réels d'ergothérapeute" AS "dont ergothérapeute",
    etra."- Dont nombre d'ETP réels d'orthophoniste" AS "dont orthophoniste",
    etra."-_dont_autre_paramedical" AS "dont autre paramédical",
    etra."ETP Psychologue" AS "Psychologue",
    cAST(etra."ETP ASH" as FLOAT) AS "ASH",
    cAST(etra."ETP Médical" as FLOAT) AS "Médical",
    cast(etra."- Dont nombre d'ETP réels de médecin coordonnateur" as FLOAT) as "dont médecin coordonnateur",
    etra."-_dont_autre_medical" AS "dont autre médical",
    cAST(etra."ETP Personnel Education nationale" as FLOAT) AS "Personnel éducation nationale",
    cAST(etra."ETP Autres fonctions" as FLOAT) AS "Autres fonctions",
    ROUND(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT)+ cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT), 2) as "Total du nombre d'ETP",
        ROUND((cAST(etra."ETP Services généraux" as FLOAT))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + cAST(etra."ETP Socio-éducatif" as float) + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2) as "Taux de personnel soins",
        ROUND((cAST(etra."ETP Socio-éducatif" as float))/(cAST(etra."ETP Direction/Encadrement" as FLOAT) + cAST(etra."ETP Administration /Gestion" as FLOAT) + cAST(etra."ETP Services généraux" as FLOAT) + cAST(etra."ETP Restauration" as FLOAT) + etra."ETP Socio-éducatif" + cAST(etra."ETP Paramédical" as FLOAT) + cAST(etra."ETP Psychologue" as FLOAT) + cAST(etra."ETP ASH" as FLOAT) + cAST(etra."ETP Médical" as FLOAT) + cAST(etra."ETP Personnel Education nationale" as FLOAT) + cAST(etra."ETP Autres fonctions" as FLOAT)),2) as "Taux de personnel socio educatif",
            CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_intellectuelles_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Déficiences intellectuelles principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Autisme_et_autres_TED_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Autisme et autres TED principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Troubles_du_psychisme_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Troubles du psychisme principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Troubles_du_langage_et_des_apprentissages_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Troubles du langage et des apprentissages principales",

        CASE 
            WHEN etra."Nombre de personnes accompagnées dans l'effectif au 31.12" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_auditives_principales" AS FLOAT) / etra."Nombre de personnes accompagnées dans l'effectif au 31.12", 2)
        END AS "Taux de résidents par Déficiences auditives principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_visuelles_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Déficiences visuelles principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_motrices_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Déficiences motrices principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Déficiences_métaboliques_viscérales_et_nutritionnelles_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Déficiences métaboliques viscérales et nutritionnelles principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Cérébro_lésions_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Cérébro lésions principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Polyhandicap_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Polyhandicap principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Troubles_du_comportement_et_de_la_communication_princiaples(TCC)" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Troubles du comportement et de la communication principales (TCC)",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Diagnostics_en_cours_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Diagnostics en cours principales",

        CASE 
            WHEN  co3.nb_lits_occ_"""+param_N_2+""" = 0 THEN NULL
            ELSE ROUND(CAST(etra."Autres_types_de_déficiences_principales" AS FLOAT) /  co3.nb_lits_occ_"""+param_N_2+""", 2)
        END AS "Taux de résidents par Autres types de déficiences principales",
        SUM(etra."% de personnes âgées de 20 - 29 ans"+
        etra."% de personnes âgées de 30 - 39 ans" +
        etra."% de personnes âgées de 40 - 49 ans"+
        etra."% de personnes âgées de 50 - 54 ans" +
        etra."% de personnes âgées de 55 - 59 ans" +
        etra."% de personnes âgées de 60 - 64 ans" +
        etra."% de personnes âgées de Plus de 65 ans") AS "Taux de résidents de plus de 20 ans" ,
        cAST(etra."Taux de vétusté des constructions" as FLOAT) AS "Taux de vétusté des constructions",
        cAST(etra."Taux de vétusté des équipements en %" as FLOAT) AS "Taux de vétusté des équipements en %",
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
        NULLTOZERO(i.'Inspection SUR SITE """+param_N_1+""" - Déjà réalisée') as 'Inspection SUR SITE """+param_N+""" - Déjà réalisée',
        NULLTOZERO(i.'Controle SUR PIECE """+param_N_1+""" - Déjà réalisé') as 'Controle SUR PIECE """+param_N+""" - Déjà réalisé',
        NULLTOZERO(i.'Inspection / contrôle Programmé """+param_N+"""') as 'Inspection / contrôle Programmé """+param_N+"""',
        MAX(CAST(SUBSTR(hsm."Date réelle Visite", 7, 4) as INTEGER)) as "Année dernière inspection (sur pl ou sur pi)"
        FROM tfiness_clean tf 
        LEFT JOIN finess_pivoted fp on fp.finess= tf.finess
        LEFT JOIN [errd_déficit_excédent_"""+param_N_4+"""] ede on tf.finess= SUBSTRING(ede."Structure - FINESS - RAISON SOCIALE", 1, 9)
        LEFT JOIN [errd_déficit_excédent_"""+param_N_3+"""] ede2 on  tf.finess = SUBSTRING(ede2."Structure - FINESS - RAISON SOCIALE", 1, 9)
        LEFT JOIN [errd_déficit_excédent_"""+param_N_2+"""] ede3 on tf.finess = SUBSTRING(ede3."Structure - FINESS - RAISON SOCIALE", 1, 9)
        LEFT JOIN communes c on c.com = tf.com_code
        LEFT JOIN departement_"""+param_N+"""  d on d.dep = c.dep
        LEFT JOIN region_"""+param_N+"""   r on d.reg = r.reg
        LEFT JOIN capacites_ehpad ce on ce."ET-N°FINESS" = tf.finess
        LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
        LEFT JOIN occupation_"""+param_N_5+"""_"""+param_N_4+""" o1 on o1.finess_19 = tf.finess
        LEFT JOIN occupation_"""+param_N_3+"""  o2  on o2.finess = tf.finess
        LEFT JOIN clean_occupation_N_2  co3  on co3.finess = tf.finess
        LEFT JOIN clean_tdb_n_2  etra on etra.finess = tf.finess
        LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
        LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
        LEFT JOIN EHPAD_Indicateurs_"""+param_N_2+"""_REG_agg eira on eira."et finess" = tf.finess
        LEFT JOIN clean_tdb_n_4  d2 on d2.finess = tf.finess
        LEFT JOIN clean_tdb_n_3  etra2 on etra2.finess = tf.finess
        LEFT JOIN recla_signalement rs on rs.finess = tf.finess
        LEFT JOIN inspections i on i.finess = tf.finess
        LEFT JOIN HELIOS_SICEA_MISSIONS_"""+param_N+""" hsm on IIF(LENGTH("Code FINESS") = 8, '0' || "Code FINESS", "Code FINESS") = tf.finess
        WHERE r.reg ='"""+str(region)+"""'
        GROUP by tf.finess
        UNION 
        SELECT
        "" as "","" as "","" AS "",""as "","" as "","" as "","" as "","" as "","" as "","" as "","" AS "","" AS "",
        "" AS "Moyenne nationale", 
        mcl."Capacité totale autorisée", 
        mcl."Type d'activité indifferencié", 
        mcl."Protection Juridique", 
        mcl."Hébergement Complet Internat", 
        mcl."Placement Famille d'Accueil", 
        mcl."Accueil temporaire avec hébergement", 
        mcl."Prestation en milieu ordinaire", 
        mcl."Accueil de jour et accompagnement en milieu ordinaire", 
        mcl."Accueil de Jour", 
        mcl."Accueil temporaire (avec et sans hébergement)", 
        mcl."Semi-Internat", 
        mcl."Hébergement de Nuit Eclaté", 
        mcl."Internat de Semaine", 
        mcl."Externat", 
        mcl."Tous modes d'accueil et d'accompagnement", 
        mcl."Tous modes d'accueil avec hébergement", 
        mcl."Accueil de Nuit", 
        mcl."Permanence téléphonique", 
        mcl."Accueil temporaire de jour", 
        mcl."Consultation Soins Externes", 
        mcl."Accueil modulable/séquentiel", 
        mcl."Traitement et Cure Ambulatoire", 
        mcl."Tous modes d'accueil (avec et sans hébergement)", 
        mcl."Equipe mobile de rue", 
        mcl."Accueil et prise en charge en appartement thérapeutique", 
        mcl."Hospitalisation Complète", 
        mcl."Regroupement Calcules (Annexes XXIV)", 
        mcl."Aide Judiciaire à la Gestion du Budget Familial", 
        mcl."Accompagnement Social Personnalisé", 
        mcl."Information des Tuteurs Familiaux", 
        mcl."Administration", 
        mcl."Taux d'occupation """+param_N_4+""" ", 
        mcl."Taux d'occupation """+param_N_3+""" ", 
        mcl."Taux d'occupation """+param_N_2+"""", 
        mcl."Nombre de résidents au 31/12/"""+param_N_2+""" ", 
        mcl."Nombre de places installées au 31/12/"""+param_N_2+""" ", 
        mcl."Taux occupation au 31/12/"""+param_N_2+""" ", 
        mcl."Part des résidents ayant plus de 10 médicaments consommés par mois", 
        mcl."Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD", 
        mcl."Taux de recours à l'hospitalisation MCO des résidents d'EHPAD", 
        mcl."Taux de recours à l'HAD des résidents d'EHPAD", 
        mcl."Total des charges", 
        mcl."Produits de la tarification", 
        mcl."Produits du compte 70", 
        mcl."Total des produits (hors c/775, 777, 7781 et 78)", 
        mcl."deficit ou excédent """+param_N_4+""" ", 
        mcl."deficit ou excédent """+param_N_3+""" ", 
        mcl."deficit ou excédent """+param_N_2+""" ", 
        mcl."Saisie des indicateurs du TDB MS (campagne """+param_N_2+""" )", 
        mcl."Taux d'absentéisme """+param_N_4+""" ", 
        mcl."Taux d'absentéisme """+param_N_3+""" ", 
        mcl."Taux d'absentéisme """+param_N_2+""" ", 
        mcl."Absentéisme moyen sur la période """+param_N_4+""" -"""+param_N_2+""" ", 
        mcl."Taux de rotation du personnel titulaire """+param_N_4+""" ", 
        mcl."Taux de rotation du personnel titulaire """+param_N_3+"""", 
        mcl."Taux de rotation du personnel titulaire """+param_N_2+""" ", 
        mcl."Rotation moyenne du personnel sur la période """+param_N_4+"""-"""+param_N_2+""" ", 
        mcl."ETP vacants """+param_N_4+""" ", 
        mcl."ETP vacants """+param_N_3+""" ", 
        mcl."ETP vacants """+param_N_2+""" ", 
        mcl."dont fonctions soins """+param_N_2+""" ", 
        mcl."dont fonctions socio-éducatives """+param_N_2+""" ", 
        mcl."Taux de prestations externes sur les prestations directes """+param_N_4+""" ", 
        mcl."Taux de prestations externes sur les prestations directes """+param_N_3+""" ", 
        mcl."Taux de prestations externes sur les prestations directes """+param_N_2+""" ", 
        mcl."Taux moyen de prestations externes sur les prestations directes", 
        mcl."Nombre total d'ETP par usager en """+param_N_4+""" ", 
        mcl."Nombre total d'ETP par usager en """+param_N_3+""" ", 
        mcl."Nombre total d'ETP par usager en """+param_N_2+""" ", 
        mcl."Nombre moyen d'ETP par usager sur la période """+param_N_4+"""-"""+param_N_2+""" ", 
        mcl."ETP 'soins' par usager en """+param_N_2+""" ", 
        mcl."Direction / Encadrement", 
        mcl."dont personnel médical d'encadrement", 
        mcl."dont autre Direction / Encadrement", 
        mcl."Administration / Gestion", 
        mcl."Services généraux", 
        mcl."Restauration", 
        mcl."Socio-éducatif", 
        mcl."dont AMP", 
        mcl."dont animateur", 
        mcl."dont moniteur éducateur", 
        mcl."dont éducateur spécialisé", 
        mcl."dont assistant(e) social(e)", 
        mcl."dont autre socio-éducatif", 
        mcl."Paramédical", 
        mcl."dont infirmier", 
        mcl."dont AES", 
        mcl."dont aide-soignant(e)", 
        mcl."dont kinésithérapeute", 
        mcl."dont psychomotricien(ne)", 
        mcl."dont ergothérapeute", 
        mcl."dont orthophoniste", 
        mcl."dont autre paramédical", 
        mcl."Psychologue", 
        mcl."ASH", 
        mcl."Médical", 
        mcl."dont médecin coordonnateur", 
        mcl."dont autre médical", 
        mcl."Personnel éducation nationale", 
        mcl."Autres fonctions", 
        mcl."Total du nombre d'ETP", 
        mcl."Taux de personnel soins", 
        mcl."Taux de personnel socio educatif", 
        mcl."Taux de résidents par Déficiences intellectuelles principales",
        mcl."Taux de résidents par Autisme et autres TED principales" ,
        mcl."Taux de résidents par Troubles du psychisme principales",
        mcl."Taux de résidents par Troubles du langage et des apprentissages principales",
        mcl."Taux de résidents par Déficiences auditives principales",
        mcl."Taux de résidents par Déficiences visuelles principales",
        mcl."Taux de résidents par Déficiences motrices principales",
        mcl."Taux de résidents par Déficiences métaboliques viscérales et nutritionnelles principales",
        mcl."Taux de résidents par Cérébro lésions principales",
        mcl."Taux de résidents par Polyhandicap principales",
        mcl."Taux de résidents par Troubles du comportement et de la communication principales (TCC)",
        mcl."Taux de résidents par Diagnostics en cours principales",
        mcl."Taux de résidents par Autres types de déficiences principales",
        mcl."Taux de résidents de plus de 20 ans", 
        mcl."Taux de vétusté des constructions", 
        mcl."Taux de vétusté des équipements en %", 
        mcl."Nombre de réclamations sur la période """+param_N_3+"""-"""+param_N+"""", 
        mcl."Rapport réclamations / capacité", 
        mcl."Recla IGAS : Hôtellerie-locaux-restauration", 
        mcl."Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service", 
        mcl."Recla IGAS : Problème de qualité des soins médicaux", 
        mcl."Recla IGAS : Problème de qualité des soins paramédicaux", 
        mcl."Recla IGAS : Recherche d’établissement ou d’un professionnel", 
        mcl."Recla IGAS : Mise en cause attitude des professionnels", 
        mcl."Recla IGAS : Informations et droits des usagers", 
        mcl."Recla IGAS : Facturation et honoraires", 
        mcl."Recla IGAS : Santé-environnementale", 
        mcl."Recla IGAS : Activités d’esthétique réglementées",
        mcl."Nombre d'EI sur la période 36mois",
        mcl."Nombre d'EIG sur la période 2021 -2024" ,
        mcl."Nombre d'EIAS sur la période 2021 -2024" ,
        mcl."Somme EI + EIGS + EIAS sur la période 2020 -2024",
        mcl."nb EI/EIG : Acte de prévention",
        mcl."nb EI/EIG : Autre prise en charge",
        mcl."nb EI/EIG : Chute",
        mcl."nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)",
        mcl."nb EI/EIG : Dispositif médical",
        mcl."nb EI/EIG : Fausse route",
        mcl."nb EI/EIG : Infection associée aux soins (IAS) hors ES",
        mcl."nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)",
        mcl."nb EI/EIG : Parcours/Coopération interprofessionnelle",
        mcl."nb EI/EIG : Prise en charge chirurgicale",
        mcl."nb EI/EIG : Prise en charge diagnostique",
        mcl."nb EI/EIG : Prise en charge en urgence",
        mcl."nb EI/EIG : Prise en charge médicamenteuse",
        mcl."nb EI/EIG : Prise en charge des cancers",
        mcl."nb EI/EIG : Prise en charge psychiatrique",
        mcl."nb EI/EIG : Suicide",
        mcl."nb EI/EIG : Tentative de suicide",
        mcl."ICE 2024 (réalisé)",
        mcl."Inspection SUR SITE """+param_N+""" - Déjà réalisée",
        mcl."Controle SUR PIECE """+param_N+""" - Déjà réalisé",
        mcl."Inspection / contrôle Programmé """+param_N+""" ",
        mcl."Année dernière inspection (sur pl ou sur pi)"
        from moyenne_controle mcl """
    cursor.execute(df_controle)
    res=cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df_controle=pd.DataFrame(res,columns=columns) 
    
    date_string = datetime.today().strftime('%d%m%Y') 
    path = 'data/output/{}_{}.xlsx'.format(utils.outputName(region),date_string)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(path, engine='xlsxwriter')
    df_ciblage.to_excel(writer, sheet_name='ciblage', index=False)
    df_controle.to_excel(writer, sheet_name='controle', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    print('export créé : {}_{}.xlsx'.format(utils.outputName(region),date_string))
    return




   