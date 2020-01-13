//LOAD DNB QUERIES
//REVISION DATE: 2020-01-08
//WHAT DOES THIS QUERY DO: 
//LOAD DNB EXPORT FILE AS CSV AND CREATE/UPDATE ALL NODES LEVEL BY LEVEL (local Supplier - DUNS - NATDUNS - GLOBALDUNS) THEN LINK THEM ACCORDINGLY

//WHAT NEEDS TO BE DONE IN ORDER FOR THE IMPORT PROCESS TO WORK PROPERLY?
//  1) IFRP IMPORT QUERIES SHOULD HAVE ALREADY BEEN EXECUTED
//  2) ALL '.CSV' file paths need to be updated according to the latest file name. (ATTENTION TO FIELDTERMINATORS)
//  3) The Headers in the CSV file must exist and be named according to the Linkurious import guid on the hub
//  4) This needs to be the second query to be run!

//WARNING: some of the following statements will invoke the 'EAGER'-Operator to prevent conflicting data-changes in subsequent operations
//this can be very memory heavy while importing files with 1,000,000 lines or more, but should not pose an issue here

//------------------------------------------------------------------------------
//------------------------------CREATE THE NODES--------------------------------
//------------------------------------------------------------------------------

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
        d.state = row.state,
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
        n.postalcode = row.du_post_code,
        n.state = row.du_state,
        n.countrycode = row.du_country_code,
        n.origin = origin             
    WITH DISTINCT n
    MERGE (d:Duns{duns:n.duns})
        ON CREATE SET
            d = n,
            d.origin = 'PLACEHOLDER';
                       
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
        g.postalcode = row.gu_post_code,
        g.state = row.gu_state,
        g.countrycode = row.gu_country_code,
        g.origin = origin
    WITH DISTINCT g
    MERGE (n:NatDuns{duns:g.duns})
        ON CREATE SET
            n = g,
            n.origin = 'PLACEHOLDER'
        WITH DISTINCT n
        MERGE (d:Duns{duns:n.duns})
            ON CREATE SET
                d = n;
                    

//------------------------------------------------------------------------------
//-------------------------CREATE THE RELATIONSHIPS-----------------------------
//------------------------------------------------------------------------------

//CREATE duns -> natduns -> gmduns
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
WITH row, row.duns_no AS duns_id
MATCH (child:Duns{duns:duns_id})
WHERE (NOT duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT duns_id IS NULL)
    WITH row, child, row.du_duns AS nat_duns_id,row.update_date AS up_date
    MATCH (father:NatDuns{duns:nat_duns_id})
    WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL) 
        WITH row, child, father, row.gu_duns AS gm_duns_id,up_date
        MATCH (gm_father:GlobalDuns{duns:gm_duns_id})
        WHERE (NOT gm_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT gm_duns_id IS NULL) 
            WITH DISTINCT (child) AS child, row, father, up_date, gm_father
            WITH DISTINCT (father) AS father, child, row, up_date, gm_father
            WITH DISTINCT (gm_father) AS gm_father, father, child, row, up_date, exists((child)-[:BELONGS]->(father)) as dn_exists
            WITH gm_father, father, child, row, up_date, dn_exists, exists((father)-[:BELONGS]->(gm_father)) as ng_exists
            OPTIONAL MATCH (child)-[t:BELONGS]->(father)-[:BELONGS]->(gm_father) WHERE t.validation_level <> 'PYD'
                SET 
                    t.validation_level = 'DNB',
                    t.origin = 'DNB_UNTRUST',
                    t.update_date = up_date
            WITH gm_father, father, child, row, up_date, dn_exists, ng_exists
            OPTIONAL MATCH (child)-[:BELONGS]->(father)-[s:BELONGS]->(gm_father) WHERE s.validation_level <> 'PYD'
                SET 
                    s.validation_level = 'DNB',
                    s.origin = 'DNB_UNTRUST',
                    s.update_date = up_date
            WITH gm_father, father, child, row, up_date, dn_exists, ng_exists
            FOREACH(cond_clause IN CASE WHEN NOT dn_exists THEN [1] ELSE [] END | 
                CREATE (child)-[y:BELONGS{origin:'DNB_UNTRUST',validation_level:'DNB',update_date:up_date}]->(father) 
                MERGE (father)-[x:BELONGS{origin:'DNB_UNTRUST'}]->(gm_father)
                    SET 
                        x.update_date = up_date,
                        x.validation_level= 'DNB'
            )
            FOREACH(cond_clause IN CASE WHEN dn_exists THEN [1] ELSE [] END | 
                FOREACH(cond_clause2 IN CASE WHEN NOT ng_exists THEN [1] ELSE [] END | 
                    CREATE (father)-[y:BELONGS{origin:'DNB_UNTRUST',validation_level:'DNB',update_date:up_date}]->(gm_father) 
                    MERGE (child)-[x:BELONGS{origin:'DNB_UNTRUST'}]->(father)
                        SET 
                            x.update_date = up_date,
                            x.validation_level ='DNB'
                )
            );   

//nat duns <- identical duns
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row fieldterminator '|' 
WITH row, row.du_duns AS nat_duns_id
MATCH (father:NatDuns{duns:nat_duns_id})
WHERE (not nat_duns_id in ['#','','NDM999999','NOH999999']) AND (not nat_duns_id IS NULL)
    WITH DISTINCT(father) AS father,row
    MATCH (child:Duns{duns:father.duns})
    WHERE NOT (child)-[:BELONGS]->(father)
        CREATE (child)-[r:BELONGS{origin:"PLACEHOLDER",validation_level:'IFR',update_date:row.update_date}]->(father);

// gm duns <- self nat duns <- self duns 
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row fieldterminator '|' 
WITH row, row.gu_duns AS gm_duns_id, row.update_date AS up_date
MATCH (father:GloablDuns{duns:gm_duns_id})
WHERE (not gm_duns_id in ['#','','NDM999999','NOH999999']) AND (NOT gm_duns_id IS NULL)
    WITH DISTINCT(father) AS father, up_date
    MATCH (child:NatDuns{duns:father.duns})
    WHERE NOT (child)-[:BELONGS]->(father)
        CREATE (child)-[r:BELONGS{origin:"PLACEHOLDER",validation_level:'DNB',update_date:up_date}]->(father)
    WITH DISTINCT(child) as father, up_date
    MATCH (child:Duns{duns:father.duns})
    WHERE NOT (child)-[:BELONGS]->(father)
        CREATE (child)-[r:BELONGS{origin:"PLACEHOLDER",validation_level:'DNB',update_date:up_date}]->(father);




//------------------------------------------------------------------------------------------------------
//-----------------------------------------------OLD----------------------------------------------------
//------------------------------------------------------------------------------------------------------

// //CREATE duns -> natduns
// LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
// WITH row, row.duns_no AS duns_id
// MATCH (child:Duns{duns:duns_id})
// WHERE (NOT duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT duns_id IS NULL)
//     WITH row, child, row.du_duns AS nat_duns_id
//     MATCH (father:NatDuns{duns:nat_duns_id})
//     WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL) 
//         WITH DISTINCT (child) AS child, row, father, exists((child)-[:BELONGS{validation_level:'PYD'}]->(:NatDuns)) as pyd_exists
//         WITH DISTINCT (father) AS father, child, row, pyd_exists, exists((child)-[:BELONGS]->(father)) as rel_exists
//             FOREACH(cond_clause IN CASE WHEN NOT rel_exists THEN [1] ELSE [] END | 
//                 //TODO:APPLY NEW DATE
//                 CREATE (child)-[y:BELONGS{origin:'DNB_UNTRUST',validation_level:'DNB',update_date:'2020-01-01'}]->(father) 
//             )
//             FOREACH(cond_clause IN CASE WHEN rel_exists THEN [1] ELSE [] END | 
//                 FOREACH(cond_clause IN CASE WHEN NOT pyd_exists THEN [1] ELSE [] END | 
//                     MERGE (child)-[y:BELONGS]->(father)
//                     SET 
//                         y.origin='DNB_UNTRUST',
//                         y.validation_level='DNB',
//                         //TODO:APPLY NEW DATE
//                         y.update_date='2020-01-01'
//                 )
//             );     
// //nat duns <- identical duns
// MATCH (father:NatDuns)
// WITH DISTINCT(father) AS father
//     MATCH (child:Duns{duns:father.duns})
//         WITH father, child
//         Where Not (child)-[:BELONGS]->(father)
//         //TODO:APPLY NEW DATE
//         CREATE (child)-[r:BELONGS{origin:"PLACEHOLDER",validation_level:'DNB',update_date:'2020-01-01'}]->(father);

// //CREATE natduns -> gmduns
// LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_dnb_export.csv' AS row FIELDTERMINATOR '|'
// WITH row, row.du_duns  AS nat_duns_id
// MATCH (child:NatDuns{duns:nat_duns_id})
// WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL)
//     WITH row, child, row.gu_duns AS gm_duns_id
//     MATCH (father:GlobalDuns{duns:gm_duns_id})
//     WHERE (NOT gm_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT gm_duns_id IS NULL)      
//         WITH DISTINCT (child) AS child, row, father, exists((child)-[:BELONGS{validation_level:'PYD'}]->(:GlobalDuns)) as pyd_exists
//         WITH DISTINCT (father) AS father, child, row, pyd_exists, exists((child)-[:BELONGS]->(father)) as rel_exists
//             FOREACH(cond_clause IN CASE WHEN NOT rel_exists THEN [1] ELSE [] END | 
//                 //TODO:APPLY NEW DATE
//                 CREATE (child)-[y:BELONGS{origin:'DNB_UNTRUST',validation_level:'DNB',update_date:'2020-01-01'}]->(father) 
//             )
//             FOREACH(cond_clause IN CASE WHEN rel_exists THEN [1] ELSE [] END | 
//                 FOREACH(cond_clause IN CASE WHEN NOT pyd_exists THEN [1] ELSE [] END | 
//                     MERGE (child)-[y:BELONGS]->(father)
//                     SET 
//                         y.origin='DNB_UNTRUST',
//                         y.validation_level='DNB',
//                         //TODO:APPLY NEW DATE
//                         y.update_date='2020-01-01'
//                 )
//             );  
// // gm duns <- self nat duns <- self duns 
// MATCH (father:GlobalDuns)
//  WITH DISTINCT(father) AS father
//  MATCH (child:NatDuns{duns:father.duns})
//         Where Not (child)-[:BELONGS]->(father)
//         //TODO:APPLY NEW DATE
//         CREATE (child)-[r:BELONGS{origin:"PLACEHOLDER",validation_level:'DNB',update_date:'2020-01-01'}]->(father)
//         WITH DISTINCT(child) as father
//          MATCH (child:Duns{duns:father.duns})
//         Where Not (child)-[:BELONGS]->(father)
//         //TODO:APPLY NEW DATE
//         CREATE (child)-[r:BELONGS{origin:"PLACEHOLDER",validation_level:'DNB',update_date:'2020-01-01'}]->(father);