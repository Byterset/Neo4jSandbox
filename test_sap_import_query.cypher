//LOAD IFRP QUERIES
//REVISION DATE: 2020-01-08
//WHAT DOES THIS QUERY DO: 
//LOAD IFRP EXPORT FILE AS CSV AND CREATE/UPDATE ALL NODES LEVEL BY LEVEL (local Supplier - DUNS - NATDUNS - GLOBALDUNS) THEN LINK THEM ACCORDINGLY

//WHAT NEEDS TO BE DONE IN ORDER FOR THE IMPORT PROCESS TO WORK PROPERLY?
//  1) Linkurious/Neo4j Database needs to be empty (easiest to achieve WITH Query: "MATCH (n) detach delete n")
//  2) ALL '.CSV' file paths need to be updated according to the latest file name. (ATTENTION TO FIELDTERMINATORS)
//  3) The Headers in the CSV file must exist and be named according to the Linkurious import guid on the hub
//  4) This needs to be the first query to be run for two reasons: 
//      4.1) It does not discriminate between relationship validation, it just adds everything according to the source file
//      4.2) The DNB Input which should be loaded directly afterwards does not contain the local supplier information 

//TODO: ADD REST OF MASTERDATA FROM FINAL FILE STRUCTURE, NOW: ONLY INFO NECESSARY FOR TESTING
//CREATE all nonexisting local suppliers WITH a unique id consisting of Source System and local ID
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|'
WITH row, (row.sr_system_id + '_' + row.sr_supplier_id) AS supplier_uid
WHERE (NOT supplier_uid IS NULL) AND (NOT row.sr_supplier_id = '') AND (NOT row.sr_system_id = '')
MERGE (child:Supplier{srUniqueID:supplier_uid}); 

//CREATE all nonexisting DUNS level nodes WITH their DUNS-ID as unique identifier
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_duns_id AS duns_id, 'IFRP' AS origin
WHERE (not duns_id in ['#','','NDM999999','NOH999999']) AND (NOT duns_id IS NULL)
MERGE (d:Duns{duns:duns_id}) 
    ON CREATE SET 
        d.origin = origin,
        d.duns = duns_id,
        d.dunsName = row.sr_supplier_duns_name;

//CREATE all nonexisting NATIONAL-DUNS level nodes WITH their DUNS-ID as unique identifier
//Also CREATE DUNS level node if nonexisting
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_national_mother_duns_id AS nat_duns_id, 'IFRP' AS origin
WHERE (not nat_duns_id in ['#','','NDM999999','NOH999999']) AND (not nat_duns_id IS NULL)
MERGE (n:NatDuns{duns:nat_duns_id}) 
    ON CREATE SET 
        n.origin = origin,
        n.duns = nat_duns_id,
        n.dunsName = row.sr_supplier_national_mother_duns_name
WITH DISTINCT n
MERGE (d:Duns{duns:n.duns})
    ON CREATE SET
        d = n,
        d.origin = 'PLACEHOLDER';

//CREATE all nonexisting GLOBAL-MOTHER-DUNS level nodes WITH their DUNS-ID as unique identifier
//Also CREATE DUNS and NATDUNS level node/s if nonexisting
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_global_mother_duns_id AS gm_duns_id, 'IFRP' AS origin
WHERE (not gm_duns_id in ['#','','NDM999999','NOH999999']) AND (not gm_duns_id IS NULL)
MERGE (g:GlobalDuns{duns:gm_duns_id}) 
    On CREATE SET 
        g.origin = origin,
        g.duns = gm_duns_id,
        g.dunsName = row.sr_supplier_global_mother_duns_name
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

//delete all previous local supplier mappings
//CREATE local suppliers to DUNS level, if there is an old relationship remove it, since new local -> Duns mapping is completely trusted.
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, (row.sr_system_id + '_' + row.sr_supplier_id) AS supplier_uid
MATCH (child:Supplier{srUniqueID:supplier_uid})
WHERE (not supplier_uid IS NULL) AND (not row.sr_supplier_id = '')
    WITH row, child, row.sr_supplier_duns_id AS duns_id
    MATCH (father:Duns{duns:duns_id})
    WHERE (NOT duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT duns_id IS NULL)
        WITH DISTINCT (child) AS child, row, father
            WITH DISTINCT (father) as father, child, row
            OPTIONAL MATCH (child)-[k:BELONGS]->(anyNode:Duns)
            WITH (CASE WHEN (anyNode.duns <> father.duns) THEN k END) AS del, father,child,row
                DELETE del
                MERGE (child)-[y:BELONGS{origin:'IFRP'}]->(father)
                    SET 
                        y.validation_level = row.source;


//CREATE duns -> natduns
//MARK ALL EXISTING RELATIONSHIPS AS PREEXISTING
MATCH (d:Duns)-[r:BELONGS]->(n:NatDuns)
WHERE r.is_preexisting = false
SET r.is_preexisting = true;                               
//Additionally: if there is not already a PYD edge existing - create the edge from the IFRP export CSV
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_duns_id AS duns_id
MATCH (child:Duns{duns:duns_id})
WHERE (NOT duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT duns_id IS NULL)
    WITH row, child, row.sr_supplier_national_mother_duns_id AS nat_duns_id
    MATCH (father:NatDuns{duns:nat_duns_id})
    WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL) 
        WITH DISTINCT (child) AS child, row, father, exists((child)-[:BELONGS{validation_level:'PYD',is_preexisting:true}]->(:NatDuns)) as pyd_exists
            WITH DISTINCT (father) AS father, child, row, pyd_exists
            //Conditional Relationships with FOREACH Clause acting as 'IF'     
            FOREACH(cond_clause IN CASE WHEN NOT pyd_exists THEN [1] ELSE [] END | 
                MERGE (child)-[y:BELONGS]->(father)
                    SET 
                        y.origin = 'IFRP',
                        y.validation_level = row.source,
                        y.update_date = row.modification_date
                    REMOVE y.is_preexisting
            )
            WITH father,child
            OPTIONAL MATCH (child)-[k:BELONGS{is_preexisting:true}]->(anyNat:NatDuns)
            WHERE k.validation_level <> 'PYD' AND (anyNat.duns <> father.duns)
            DELETE k;
//nat duns <- identical duns
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_national_mother_duns_id AS nat_duns_id
MATCH (father:NatDuns{duns:nat_duns_id})
WHERE (not nat_duns_id in ['#','','NDM999999','NOH999999']) AND (not nat_duns_id IS NULL)
WITH DISTINCT(father) AS father
    MATCH (child:Duns{duns:father.duns})
        WITH father, child
        Where Not (child)-[:BELONGS]->(father)
        //TODO:APPLY NEW DATE
        CREATE (child)-[r:BELONGS{origin:"PLACEHOLDER",validation_level:'IFR',update_date:'2020-01-01'}]->(father);

MATCH (d:Duns)-[r:BELONGS]->(n:NatDuns)
WHERE (NOT r.is_preexisting IS NULL)
REMOVE r.is_preexisting;  



//CREATE natduns -> gmduns
//MARK ALL EXISTING RELATIONSHIPS AS PREEXISTING
MATCH (d:NatDuns)-[r:BELONGS]->(n:GlobalDuns)
WHERE r.is_preexisting = false
SET r.is_preexisting = true;    
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_national_mother_duns_id  AS nat_duns_id
MATCH (child:NatDuns{duns:nat_duns_id})
WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL)
    WITH row, child, row.sr_supplier_global_mother_duns_id AS gm_duns_id
    MATCH (father:GlobalDuns{duns:gm_duns_id})
    WHERE (NOT gm_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT gm_duns_id IS NULL)      
        WITH DISTINCT (child) AS child, row, father, exists((child)-[:BELONGS{validation_level:'PYD', is_preexisting:true}]->(:GlobalDuns)) as pyd_exists
        WITH DISTINCT (father) AS father, child, row, pyd_exists
            FOREACH(cond_clause IN CASE WHEN NOT pyd_exists THEN [1] ELSE [] END | 
                MERGE (child)-[y:BELONGS]->(father)
                    SET 
                        y.origin = 'IFRP',
                        y.is_preexisting = false,
                        y.validation_level = row.source,
                        y.update_date = row.modification_date
            )
            WITH father,child
            OPTIONAL MATCH (child)-[k:BELONGS{is_preexisting:true}]->(anyGlob:GlobalDuns)
            WHERE k.validation_level <> 'PYD' AND (anyGlob.duns <> father.duns)
                DELETE k;   
// gm duns <- self nat duns <- self duns 
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_national_mother_duns_id AS gm_duns_id
MATCH (father:GloablDuns{duns:gm_duns_id})
WHERE (not gm_duns_id in ['#','','NDM999999','NOH999999']) AND (NOT gm_duns_id IS NULL)
 WITH DISTINCT(father) AS father
 MATCH (child:NatDuns{duns:father.duns})
        Where Not (child)-[:BELONGS]->(father)
        //TODO:APPLY NEW DATE
        CREATE (child)-[r:BELONGS{origin:"PLACEHOLDER",validation_level:'IFR',update_date:'2020-01-01'}]->(father)
        WITH DISTINCT(child) as father
         MATCH (child:Duns{duns:father.duns})
        Where Not (child)-[:BELONGS]->(father)
        //TODO:APPLY NEW DATE
        CREATE (child)-[r:BELONGS{origin:"PLACEHOLDER",validation_level:'IFR',update_date:'2020-01-01'}]->(father);
MATCH (d:NatDuns)-[r:BELONGS]->(n:GlobalDuns)
WHERE (NOT r.is_preexisting IS NULL)
REMOVE r.is_preexisting;  
