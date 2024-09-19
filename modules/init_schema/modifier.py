import pandas as pd
import os

def modifier_finess(input_file_path, output_file_path, colonne_names):
    """
    Modifie un fichier CSV spécifique pour le format "finess".
    Supprime les 49 299 premières lignes et ajoute une ligne d'en-tête.

    :param input_file_path: Chemin complet du fichier d'entrée
    :param output_file_path: Chemin complet du fichier de sortie
    :param colonne_names: Liste des noms de colonnes à insérer
    """
    # Lire le fichier à partir de la 49300ème ligne (0-indexed) en utilisant ';' comme séparateur
    df = pd.read_csv(input_file_path, skiprows=49299, header=None, sep=';', on_bad_lines='skip')

    # Afficher le nombre de colonnes dans la DataFrame
    print(f"Nombre de colonnes trouvées: {df.shape[1]}")

    # Vérifier si le nombre de colonnes correspond au nombre de noms spécifiés
    if df.shape[1] != len(colonne_names):
        raise ValueError(f"Le nombre de colonnes dans le fichier ({df.shape[1]}) ne correspond pas au nombre de noms de colonnes spécifiés ({len(colonne_names)}).")

    # Insérer la nouvelle ligne de noms de colonnes
    df.columns = colonne_names

    # Créer le dossier de sortie s'il n'existe pas
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    # Enregistrer le fichier modifié
    df.to_csv(output_file_path, index=False, sep=';')

    print(f"Fichier modifié enregistré sous: {output_file_path}")
