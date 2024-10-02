sql_query = """
CREATE TABLE charges_produits AS 
SELECT 
    cor.finess, 
    CASE 
        WHEN cor.CADRE = 'ERRD' THEN gec.sum_charges_dexploitation 
        WHEN cor.CADRE = 'CA PA' THEN gc.sum_charges_dexploitation 
        WHEN cor.CADRE = 'CA PH' THEN gcch.sum_charges_dexploitation 
    END as 'Total des charges', 
    CASE 
        WHEN cor.CADRE = 'ERRD' THEN gep.sum_groupe_i__produits_de_la_tarification 
        WHEN cor.CADRE = 'CA PA' THEN gcp.sum_groupe_i__produits_de_la_tarification 
        WHEN cor.CADRE = 'CA PH' THEN gcch2.sum_groupe_i__produits_de_la_tarification 
    END as 'Produits de la tarification', 
    CASE 
        WHEN cor.CADRE = 'ERRD' THEN gep2.sum_produits70 
        WHEN cor.CADRE = 'CA PA' THEN 0 
        WHEN cor.CADRE = 'CA PH' THEN gcch3.sum_produits70 
    END as 'Produits du compte 70', 
    CASE 
        WHEN cor.CADRE = 'ERRD' THEN gep3.sum_produits_dexploitation 
        WHEN cor.CADRE = 'CA PA' THEN 0 
        WHEN cor.CADRE = 'CA PH' THEN gcch4.sum_produits_dexploitation 
    END as 'Total des produits (hors c/775, 777, 7781 et 78)' 
FROM 
    correspondance cor 
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
