import argparse
from modules.init_db.init_db import init_db, conn_db
from utils import utils
from modules.transform.transform import execute_transform, init_table
from modules.export.export import create_export
from modules.importsource.import_data import import_from_sftp
from modules.create_csv.create_csv import create_csv_function
from modules.load_csv.load_csv_to_db import load_csv_to_db

def main(args):
    if args.commande == "import_file":
        import_from_sftp()
    elif args.commande == "create_csv":
        create_csv_function()
    elif args.commande == "init_database":
        exe_db_init()
    elif args.commande == "load_csv":
        load_csv_to_db()
    elif args.commande == "transform":
        if args.region is None:
            print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif args.region == 0:
            list_region = utils.read_settings('settings/settings.json', "region", "code")
            for r in list_region:
                transform(r)
        else:
            transform(args.region)
    elif args.commande == "export":
        if args.region is None:
            print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif args.region == 0:
            list_region = utils.read_settings('settings/settings.json', "region", "code")
            for r in list_region:
                export_region(r)
        else:
            export_region(args.region)
    elif args.commande == "all":
        all_functions(args.region)

def import_file():
    import_from_sftp()
    pass

def exe_db_init():
    dbname = utils.read_settings('settings/settings.json', "db", "name")
    conn = init_db(dbname)
    conn.close()
    print("Base de données initialisée avec succès.")

def create_csv():
    create_csv_function()

def transform(region):
    dbname = utils.read_settings("settings/settings.json", "db", "name")
    conn = conn_db(dbname)
    params = utils.read_settings('settings/settings.json', 'parametres', 'param_N')
    
    init_table(conn)
    print("Table initialisée avec succès.")
    
    execute_transform(region)
    print(f"Transformation exécutée pour la région {region}.")
    
    conn.close()

def export_region(region):
    try:
        print(f"Export des données pour la région {region}.")
        create_export(region)  # Appel à la fonction d'exportation du module
        print(f"Export réussi pour la région {region}.")
    except Exception as e:
        print(f"Erreur lors de l'exportation : {e}")


def all_functions(region):
    #import_file()
    exe_db_init()
    create_csv()
    load_csv_to_db()
    
    if region == 0:
        list_region = utils.read_settings('settings/settings.json', "region", "code")
        print(f"Liste des régions lues : {list_region}")
        for r in list_region:
            print(f"Traitement de la région : {r}")
            transform(r)  # Transformation
            #create_export(r)  # Export des fichiers vers SFTP après transformation
    else:
        print(f"Traitement de la région spécifiée : {region}")
        transform(region)  # Transformation
        #create_export(region)  # Export des fichiers vers SFTP après transformation

parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
parser.add_argument("region", type=int, help="Code région pour filtrer")
args = parser.parse_args()

if __name__ == "__main__":
    main(args)
