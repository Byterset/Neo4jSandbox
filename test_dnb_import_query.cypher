//LOAD DNB QUERIES
//REVISION DATE: 2020-01-04
//WHAT DOES THIS QUERY DO: 
//LOAD DNB EXPORT FILE AS CSV AND CREATE/UPDATE ALL NODES LEVEL BY LEVEL (local Supplier - DUNS - NATDUNS - GLOBALDUNS) THEN LINK THEM ACCORDINGLY

//WHAT NEEDS TO BE DONE IN ORDER FOR THE IMPORT PROCESS TO WORK PROPERLY?
//  1) DNB_UNTRUST IMPORT QUERIES SHOULD HAVE ALREADY BEEN EXECUTED
//  2) ALL '.CSV' file paths need to be updated according to the latest file name. (ATTENTION TO FIELDTERMINATORS)
//  3) The Headers in the CSV file must exist and be named according to the Linkurious import guid on the hub
//  4) This needs to be the first query to be run for two reasons: 
//      4.1) It does not discriminate between relationship validation, it just adds everything according to the source file
//      4.2) The DNB Input which should be loaded directly afterwards does not contain the local supplier information 

//TODO: ADD REST OF MASTERDATA FROM FINAL FILE STRUCTURE, NOW: ONLY INFO NECESSARY FOR TESTING


// CREATE THE NODES
//CREATE all nonexisting DUNS level nodes WITH their DUNS-ID as unique identifier
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

//CREATE all nonexisting NATIONAL-DUNS level nodes WITH their DUNS-ID as unique identifier
//Also CREATE DUNS level node if nonexisting
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
                       
//CREATE all nonexisting GLOBAL-MOTHER-DUNS level nodes WITH their DUNS-ID as unique identifier
//Also CREATE DUNS and NATDUNS level node/s if nonexisting
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

//------------------------------------------------------------------------------
//-------------------------CREATE THE RELATIONSHIPS-----------------------------
//------------------------------------------------------------------------------
//CREATE duns -> natduns
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
WITH row, row.duns_no AS duns_id
MATCH (child:Duns{duns:duns_id})
WHERE (NOT duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT duns_id IS NULL)
    WITH row, child, row.du_duns AS nat_duns_id
    MATCH (father:NatDuns{duns:nat_duns_id})
    WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL) 
        WITH DISTINCT (child) AS child, row, father, exists((child)-[:BELONGS{validation_level:'PYD'}]->(:NatDuns)) as pyd_exists
        WITH DISTINCT (father) AS father, child, row, pyd_exists
            //Conditional Relationships with FOREACH Clause acting as 'IF'
            OPTIONAL MATCH (child)-[k:BELONGS{origin:'DNB_UNTRUST'}]->(anyNode:NatDuns)
            WITH (CASE WHEN (anyNode.duns <> father.duns) THEN k END) AS del, father,child,row,pyd_exists
            FOREACH(cond_clause IN CASE WHEN NOT pyd_exists THEN [1] ELSE [] END | 
                DELETE del
                MERGE (child)-[y:BELONGS{origin:'DNB_UNTRUST'}]->(father)
                    SET 
                        y.validation_level = 'DNB',
                        y.update_date = '2020-01-01' //TODO:APPLY NEW DATE
            )
            //IF there is an 'PYD' level relationship existing: delete obsolete other relationships and ignore CSV rel. since PYD is highest value
            WITH child, pyd_exists
            OPTIONAL MATCH (child)-[k:BELONGS{origin:'DNB_UNTRUST'}]->(:NatDuns) 
            WHERE pyd_exists AND k.validation_level <> 'PYD' 
            FOREACH(cond_clause IN CASE WHEN pyd_exists THEN [1] ELSE [] END | 
                    DELETE k
            );        
// nat duns <- identical duns
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
WITH row, row.du_duns AS nat_duns_id
MATCH (father:NatDuns{duns:nat_duns_id})
WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL) 
    WITH DISTINCT(father) AS father
    OPTIONAL MATCH (child:Duns{duns:father.duns})-[k:BELONGS{origin:'DNB_UNTRUST'}]->(anyNode:NatDuns)
            WHERE  anyNode.duns <> father.duns DELETE k
    WITH father
    MATCH (child:Duns{duns:father.duns})
        WITH father, child
        Where Not (child)-[:BELONGS]->(father)
        //TODO: APPLY NEW DATE
        CREATE (child)-[r:BELONGS{origin:"DNB_UNTRUST",validation_level:'DNB',update_date:'2020-01-01'}]->(father);



//CREATE natduns -> gmduns
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
WITH row, row.du_duns  AS nat_duns_id
MATCH (child:NatDuns{duns:nat_duns_id})
WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL)
    WITH row, child, row.gu_duns AS gm_duns_id
    MATCH (father:GlobalDuns{duns:gm_duns_id})
    WHERE (NOT gm_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT gm_duns_id IS NULL)      
        WITH DISTINCT (child) AS child, row, father, exists((child)-[:BELONGS{validation_level:'PYD'}]->(:GlobalDuns)) as pyd_exists
        WITH DISTINCT (father) AS father, child, row, pyd_exists
            //Conditional Relationships with FOREACH Clause acting as 'IF'
            OPTIONAL MATCH (child)-[k:BELONGS{origin:'DNB_UNTRUST'}]->(anyNode:GlobalDuns)
            //IF there is no existing 'PYD' level relationship: delete rels not between child and father based on CSV
            //Then Merge new Connection
            WITH CASE WHEN anyNode.duns <> father.duns THEN k END AS del, father,child,row,pyd_exists
            FOREACH(cond_clause IN CASE WHEN NOT pyd_exists THEN [1] ELSE [] END | 
                DELETE del
                MERGE (child)-[y:BELONGS{origin:'DNB_UNTRUST'}]->(father)
                    SET 
                        y.validation_level = 'DNB',
                        y.update_date = '2020-01-01' //TODO: APPLY NEW DATE
            )
            //IF there is an 'PYD' level relationship existing: delete obsolete other relationships and ignore CSV rel. since PYD is highest value
            WITH child, pyd_exists
            OPTIONAL MATCH (child)-[k:BELONGS{origin:'DNB_UNTRUST'}]->(:GlobalDuns) 
            WHERE pyd_exists AND k.validation_level <> 'PYD' 
            FOREACH(cond_clause IN CASE WHEN pyd_exists THEN [1] ELSE [] END | 
                    DELETE k
            );        
// gm duns <- self nat duns <- self duns 
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
WITH row, row.gu_duns AS gm_duns_id
MATCH (father:GlobalDuns{duns:gm_duns_id})
WHERE (NOT gm_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT gm_duns_id IS NULL)
    WITH DISTINCT(father) AS father
    //DELETE obsolete relationships to other nodes, if child-DUNS = father-DUNS the relationship is trivial
    OPTIONAL MATCH (child:NatDuns{duns:father.duns})-[k:BELONGS{origin:'DNB_UNTRUST'}]->(anyNode:GlobalDuns)
            WHERE  anyNode.duns <> father.duns DELETE k
    WITH father
    MATCH (child:NatDuns{duns:father.duns})
        WITH father, child
        Where Not (child)-[:BELONGS]->(father)
        CREATE (child)-[r:BELONGS{origin:"DNB_UNTRUST",validation_level:'PYD',update_date:'2020-01-01'}]->(father) //TODO: APPLY NEW DATE
            WITH DISTINCT(child) as father //go one level deeper, now NatDuns as father with child Duns
            OPTIONAL MATCH (child:Duns{duns:father.duns})-[k:BELONGS{origin:'DNB_UNTRUST'}]->(anyNode:NatDuns)
            WHERE  anyNode.duns <> father.duns DELETE k
                WITH father
                MATCH (child:Duns{duns:father.duns})
                    WITH father, child
                    Where Not (child)-[:BELONGS]->(father)
                    //TODO: APPLY NEW DATE
                    CREATE (child)-[r:BELONGS{origin:"DNB_UNTRUST",validation_level:'PYD',update_date:'2020-01-01'}]->(father); 

