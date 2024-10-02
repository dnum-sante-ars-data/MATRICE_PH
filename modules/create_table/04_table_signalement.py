sql_query= """
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