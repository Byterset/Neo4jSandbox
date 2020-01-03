// CREATE THE NODES
//dunsid
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
WITH row, row.duns_no as dunsid, 'DNB_UNTRUST' AS origin
WHERE NOT dunsid  IN [ '#', '','NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
MERGE (d:Duns{duns:dunsid})
    ON CREATE SET
        d.duns = dunsid,
        d.dunsName = row.name,
        d.street = row.add,
        d.city = row.city,
        d.postalcode = row.post_code,
        d.countrycode = row.country_code,
        d.origin = origin;

//natdunsid and its duns
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
WITH row, row.du_duns as dunsid, 'DNB_UNTRUST' AS origin
WHERE NOT dunsid  IN [ '#','', 'NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
MERGE (n:NatDuns{duns:dunsid})
    ON CREATE SET
        n.duns = dunsid,
        n.dunsName = row.du_name,
        n.street = row.du_add,
        n.city= row.du_city,
        n.postalcode = row.nat_postalcode,
        n.countrycode = row.du_country_code,
        n.origin = origin             
    WITH DISTINCT n
    MERGE (d:Duns{duns:n.duns})
        ON CREATE SET
            d = n;
                       
// gmduns and natduns and duns
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
WITH row, row.gu_duns as dunsid, 'DNB_UNTRUST' AS origin
WHERE NOT dunsid  IN [ '#', '', 'NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
MERGE (g:GlobalDuns{duns:dunsid})
    ON CREATE SET
        g.duns = dunsid,
        g.dunsName = row.gu_name,
        g.street = row.gu_add,
        g.city= row.gu_city,
        g.postalcode = row.gu_postalcode,
        g.countrycode = row.gu_country_code,
        g.origin = origin
    WITH DISTINCT g
        MERGE (n:NatDuns{duns:g.duns})
            ON CREATE SET
                n = g    
            WITH DISTINCT n
            MERGE (d:Duns{duns:n.duns})
                ON CREATE SET
                    d = n;
                    
// CREATE THE RELATIONSHIPS (IF THERE IS NOT A HIGHER VALUE RELATIONSHIP EXISTING) 
// duns -> natduns
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
WITH row, row.duns_no as dunsid
        MATCH (child:Duns{duns:dunsid})
        WHERE NOT dunsid  IN [ '#','', 'NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH row, child, row.du_duns as dunsid
        MATCH (father:NatDuns{duns:dunsid})
        WHERE NOT dunsid  IN [ '#','', 'NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH DISTINCT (child) as child, father
WITH DISTINCT(father) as father, child
        MERGE (child)-[r:BELONGS{source:"DNB"}]->(father)
        SET 
               r.update_date = '2019-02-18';









// natduns -> gmduns
LOAD CSV WITH HEADERS FROM 'file:///DNB_FINAL_Merged_Files_1812_1902_V2.csv' AS row FIELDTERMINATOR '|'
WITH row, row.du_duns  as dunsid
        MATCH (child:NatDuns{supplierlocalid:dunsid})
        WHERE NOT dunsid  IN [ '#', '', 'NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH row, child, row.gu_duns as dunsid
        MATCH (father:GlobalDuns{supplierlocalid:dunsid})
        WHERE NOT dunsid  IN [ '#', '','NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH DISTINCT (child) as child, father
WITH DISTINCT(father) as father, child
        MERGE (child)-[r:BELONGS{source:"DNB"}]->(father)
        SET 
               r.update_date = '2019-02-18';
        
// nat duns <- self duns 
LOAD CSV WITH HEADERS FROM 'file:///DNB_FINAL_Merged_Files_1812_1902_V2.csv' AS row FIELDTERMINATOR '|'
WITH row, row.du_duns as dunsid
        MATCH (father:NatDuns{supplierlocalid:dunsid})
        WHERE NOT dunsid  IN [ '#', '','NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH DISTINCT(father) as father
        MATCH (child:Duns{supplierlocalid:father.supplierlocalid})
WITH father, child
        MERGE (child)-[r:BELONGS{source:"DNB"}]->(father)
        SET 
               r.update_date = '2019-02-18';
        
// gm duns <- self nat duns <- self duns 
LOAD CSV WITH HEADERS FROM 'file:///DNB_FINAL_Merged_Files_1812_1902_V2.csv' AS row FIELDTERMINATOR '|'
WITH row, row.gu_duns as dunsid
        MATCH (father:GmDuns{supplierlocalid:dunsid})
        WHERE NOT dunsid  IN [ '#', '', 'NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH DISTINCT(father) as father
        MATCH (child:NatDuns{supplierlocalid:father.supplierlocalid})
WITH father, child
        MERGE (child)-[r:BELONGS{source:"DNB"}]->(father)
        SET 
               r.update_date = '2019-02-18'
WITH DISTINCT(child) as father
        MATCH (child:Duns{supplierlocalid:father.supplierlocalid})
WITH father, child
        MERGE (child)-[r:BELONGS{source:"DNB"}]->(father)
        SET 
               r.update_date = '2019-02-18';

//register id
LOAD CSV WITH HEADERS FROM 'file:///DNB_FINAL_Merged_Files_1812_1902_V2.csv' AS row FIELDTERMINATOR '|'
WITH row, row.duns_no as dunsid
        MATCH (d:Duns{supplierlocalid:dunsid})
        WHERE
                       (NOT row.nat_id IS NULL)
               AND 
                       (NOT dunsid  IN [ '#', '', 'NDM999999', 'NOH999999'])
               AND 
                       (dunsid IS NOT NULL)
WITH row, d
               MERGE (t:TaxID{taxcode:row.nat_id})
WITH d, t
        MERGE (d)-[r:HASID{source:"DNB"}]->(t)
        SET
               r.source = "DNB",
               r.update_date = '2019-02-18',
               r.type = "registerid"
               
// old duns
LOAD CSV WITH HEADERS FROM 'file:///duns_to_remove.csv' AS row FIELDTERMINATOR '|'
WITH row, row.duns_no as dunsid
        MATCH (d:Duns{supplierlocalid:dunsid})
        WHERE (NOT dunsid  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT dunsid IS NULL)
WITH DISTINCT d
        SET
               d.obsolete = True,
               d.source = "DNB",
               d.update_date = '2019-02-18';

// reallocated duns
LOAD CSV WITH HEADERS FROM 'file:///duns_to_renew.csv' AS row FIELDTERMINATOR '|'
WITH row.old_duns as old_duns, row.new_duns AS new_duns
        MATCH (od:Duns{supplierlocalid:old_duns}),  (nd:Duns{supplierlocalid:new_duns})
        WHERE
                       (NOT old_duns  IN [ '#', '', 'NDM999999', 'NOH999999']) AND (old_duns IS NOT NULL)
               AND 
                       (NOT new_duns  IN [ '#', '','NDM999999', 'NOH999999']) AND (new_duns IS NOT NULL)
WITH od, nd
        MERGE (od)-[r:HASID{source:"DNB"}]->(nd)
               SET
                       r.type = "duns",
                       r.update_date = '2019-02-18';
