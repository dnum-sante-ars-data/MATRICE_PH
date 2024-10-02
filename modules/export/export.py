import os
import paramiko
from datetime import datetime
import json
from utils import utils
import logging

# Configuration de la journalisation
logging.basicConfig(level=logging.INFO)

def create_export(region):
    try:
        logging.info("Début de la fonction create_export")

        # Récupérer les informations SFTP
        url, username, private_key_path = utils.sftpInfo()
        
        logging.info(f"Configuration SFTP : URL={url}, Username={username}, Private Key Path={private_key_path}")

        # Charger les chemins depuis le fichier JSON (sans conversion des barres)
        localpath_base = utils.read_settings('settings/settings.json', 'sftp', 'localpath_base')
        remotepath_base = utils.read_settings('settings/settings.json', 'sftp', 'remotepath_base')

        logging.info(f"Local Path Base: {localpath_base}, Remote Path Base: {remotepath_base}")

        # Connexion SSH avec la clé privée
        private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
        logging.info("Clé privée chargée avec succès")
        
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(url, username=username, pkey=private_key)
        logging.info("Connexion SSH établie")

        # Récupérer la date d'aujourd'hui pour nommer le fichier
        date_string = datetime.today().strftime('%d%m%Y')
        region_name = utils.outputName(region)
        
        if region_name is None:
            logging.error("La région est invalide, sortie de la fonction.")
            return 

        # Construire les chemins sans modifier les barres obliques dans les chemins locaux
        localpath = f'{localpath_base}{region_name}_{date_string}.xlsx'
        remotepath = f'{remotepath_base}{region_name}_{date_string}.xlsx'

        logging.info(f"Chemin local du fichier : {localpath}")
        logging.info(f"Chemin distant du fichier : {remotepath}")

        # Vérifier si le fichier local existe
        if not os.path.exists(localpath):
            logging.error(f"Erreur : Le fichier local n'existe pas : {localpath}")
            logging.info("Contenu du dossier de sortie :")
            logging.info(os.listdir(localpath_base))
            return
        
        # Transférer le fichier via SFTP
        with ssh.open_sftp() as sftp:
            logging.info("Connexion SFTP ouverte")
            logging.info(f"Tentative de transfert du fichier depuis {localpath} vers {remotepath}")
            sftp.put(localpath, remotepath)
            logging.info(f"Fichier {localpath} transféré avec succès à {remotepath}")

        logging.info("Fermeture de la connexion SSH")
        ssh.close()

    except Exception as e:
        logging.error(f"Erreur lors de l'établissement de la connexion ou du transfert : {str(e)}")
        logging.exception("Exception détaillée")
