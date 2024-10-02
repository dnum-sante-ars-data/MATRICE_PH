sql_query = """
CREATE TABLE communes AS 
SELECT 
    c.com, c.dep, c.ncc   
FROM 
    commune_{param_N} c  
WHERE 
    c.reg IS NOT NULL 
UNION 
ALL SELECT 
    c.com, c2.dep, c.ncc 
FROM 
    commune_{param_N} c  
    LEFT JOIN commune_{param_N} c2 on c.comparent = c2.com AND c2.dep IS NOT NULL 
WHERE 
    c.reg IS NULL and c.com != c.comparent
"""
