from utils.utils import convertXlsxToCsv
import re
from os import listdir
from modules.init_schema.modifier import modifier_finess  # Assurez-vous que modifier_finess est importé
import os

def create_csv():
    # Définition des variables
    input_folder = 'data/input'
    output_folder = 'data/to_csv'
    demo_file_pattern = r'demo.csv|demo.xlsx'

    # Liste des dossiers dans le répertoire d'entrée
    all_folders = listdir(input_folder)

    for folder_name in all_folders:
        folder_path = f'{input_folder}/{folder_name}'
        all_files = listdir(folder_path)

        for input_file_name in all_files:
            input_file_path = f'{folder_path}/{input_file_name}'
            output_file_name = f'{input_file_name.split(".")[0]}.csv'
            output_file_path = f'{output_folder}/{output_file_name}'

            # Ignorer les fichiers de démonstration
            if re.search(demo_file_pattern, input_file_name):
                print(f'{input_file_name} not added (demo file)')
            
            # Cas spécifique pour les fichiers finess.csv
            elif "finess.csv" in input_file_name.lower():
                print(f'Processing finess file: {input_file_name}')
                colonne_names = [
                    "Section : equipementsocial", "Numéro FINESS ET", "Code de la discipline d'équipement",
                    "Libellé de la discipline d'équipement", "Code d'activité", "Libellé du code d'activité", "Code clientèle",
                    "Libellé du code clientèle", "Code source information", "Capacité installée totale",
                    "Capacité installation hommes", "Capacité installation femmes", "Capacité installation habilités aide sociale",
                    "Age minimum installation", "Age maximum installation", "Indicateur de suppression de l’installation (O/N)",
                    "Date de dernière installation", "Date de première autorisation", "Capacité autorisée totale", "Capacité autorisation hommes",
                    "Capacité autorisation femmes", "Capacité autorisation habilités aide sociale", "Age minimum autorisation",
                    "Age maximum autorisation", "Indicateur de suppression de l’autorisation (O/N)", "Date d'autorisation",
                    "Date MAJ de l’autorisation", "Date MAJ de l’installation"
                ]
                modifier_finess(input_file_path, output_file_path, colonne_names)
            
            # Cas spécifique pour les fichiers t-finess.xlsx
            elif "t-finess.xlsx" in input_file_name.lower():
                print(f'Processing t-finess file: {input_file_name}')
                convertXlsxToCsv(input_file_path, output_file_path)
                print(f'Converted Excel file and added: {input_file_name}')
            
            # Conversion des fichiers Excel en CSV
            elif input_file_name.lower().endswith('.xlsx'):
                convertXlsxToCsv(input_file_path, output_file_path)
                print(f'Converted Excel file and added: {input_file_name}')
            
            # Fichiers CSV : pas de conversion nécessaire, juste un message
            elif input_file_name.lower().endswith('.csv'):
                print(f'CSV file already exists and was added: {input_file_name}')


