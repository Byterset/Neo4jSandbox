//LOAD IFRP QUERY
//REVISION DATE: 2020-01-02
//WHAT DOES THIS QUERY DO: 
//LOAD IFRP EXPORT FILE AS CSV AND CREATE/UPDATE ALL NODES LEVEL BY LEVEL (local Supplier - DUNS - NATDUNS - GLOBALDUNS) THEN LINK THEM ACCORDINGLY

//WHAT NEEDS TO BE DONE IN ORDER FOR THE IMPORT PROCESS TO WORK PROPERLY?
//  1) Linkurious/Neo4j Database needs to be empty (easiest to achieve WITH Query: "MATCH (n) detach delete n")
//  2) ALL '.CSV' file paths need to be updated according to the latest file name. (ATTENTION TO FIELDTERMINATORS)
//  3) The Headers in the CSV file must exist and be named according to the Linkurious import guid on the hub
//  4) This needs to be the first query to be run for two reasons: 
//      4.1) It does not discriminate between relationship validation, it just adds everything according to the source file
//      4.2) The DNB Input which should be loaded directly afterwards does not contain the local supplier information 

//TODO: ADD REST OF MASTERDATA FROM REAL FILE

//CREATE all nonexisting local suppliers WITH a unique id consisting of Source System and local ID
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, apoc.util.md5([row.sr_system_id + '_' + row.sr_supplier_id]) AS supplier_uid
WHERE (NOT supplier_uid IS NULL) AND (NOT row.sr_supplier_id = '')
MERGE (child:Supplier{srUniqueID:supplier_uid}) 
    ON CREATE SET 
        child.systemID = row.sr_system_id,
        child.supplierID = row.sr_supplier_id,
        child.srUniqueID = row.sr_system_id + '_' + row.sr_supplier_id;

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
            d.origin = n.origin,
            d.duns = n.duns,
            d.dunsName = n.dunsName;

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
            n.origin = g.origin,
            n.duns = g.duns,
            n.dunsName = g.dunsName
        WITH DISTINCT n
        MERGE (d:Duns{duns:n.duns})
            ON CREATE SET
                d.origin = n.origin,
                d.duns = n.duns,
                d.dunsName = n.dunsName;

// CREATE THE RELATIONSHIPS
//local suppliers to DUNS level
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, apoc.util.md5([row.sr_system_id + '_' + row.sr_supplier_id]) AS supplier_uid
MATCH (child:Supplier{srUniqueID:supplier_uid})
WHERE (not supplier_uid IS NULL) AND (not row.sr_supplier_id = '')
    WITH row, child, row.sr_supplier_duns_id AS duns_id
    MATCH (father:Duns{duns:duns_id})
    WHERE (NOT duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT duns_id IS NULL)
        WITH DISTINCT (child) AS child, father, row
            WITH DISTINCT(father) AS father, child, row
            MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
                SET 
                    r.validation_level = row.source,
                    r.update_date = row.modification_date;                       
   
//duns -> natduns
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_duns_id AS duns_id
MATCH (child:Duns{duns:duns_id})
WHERE (NOT duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT duns_id IS NULL)
    WITH row, child, row.sr_supplier_national_mother_duns_id AS nat_duns_id
    MATCH (father:NatDuns{duns:nat_duns_id})
    WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL) 
        WITH DISTINCT (child) AS child, father, row
            WITH DISTINCT(father) AS father, child, row
            MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
                SET 
                    r.validation_level = row.source,
                    r.update_date = row.modification_date;
// nat duns <- identical duns
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_national_mother_duns_id AS nat_duns_id
MATCH (father:NatDuns{duns:nat_duns_id})
WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL) 
    WITH DISTINCT(father) AS father
    MATCH (child:Duns{duns:father.duns})
        WITH father, child
        Where Not (child)-[:BELONGS]-(father)
        MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
            SET 
            //TODO: Change these accordingly!
                r.validation_level = 'PYD', //arbitrary values: recommend current date and PYD level verification
                r.update_date = '2020-01-02'; 

// natduns -> gmduns
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_national_mother_duns_id  AS nat_duns_id
MATCH (child:NatDuns{duns:nat_duns_id})
WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL)
    WITH row, child, row.sr_supplier_global_mother_duns_id AS gm_duns_id
    MATCH (father:GlobalDuns{duns:gm_duns_id})
    WHERE (NOT gm_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT gm_duns_id IS NULL)
        WITH DISTINCT (child) AS child, father, row
            WITH DISTINCT(father) AS father, child, row
            MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
                SET 
                    r.validation_level = row.source,
                    r.update_date = row.modification_date;
// gm duns <- self nat duns <- self duns 
Load CSV WITH headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' AS row fieldterminator '|' 
WITH row, row.sr_supplier_global_mother_duns_id AS gm_duns_id
MATCH (father:GlobalDuns{duns:gm_duns_id})
WHERE (NOT gm_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT gm_duns_id IS NULL)
    WITH DISTINCT(father) AS father
    MATCH (child:NatDuns{duns:father.duns})
        WITH father, child
        Where Not (child)-[:BELONGS]-(father)
        MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
            SET 
            //TODO: Change these accordingly!
                r.validation_level = 'PYD', //arbitrary values: recommend current date and PYD level verification
                r.update_date = '2020-01-02'
            WITH DISTINCT(child) as father
            MATCH (child:Duns{duns:father.duns})
                WITH father, child
                Where Not (child)-[:BELONGS]-(father)
                MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
                    SET 
                    //TODO: Change these accordingly!
                        r.validation_level = 'PYD', //arbitrary values: recommend current date and PYD level verification
                        r.update_date = '2020-01-02'; 
              
//invoice and order volume
//LOAD CSV WITH HEADERS FROM 'file:///YP91FILEL_20190211.csv' AS row FIELDTERMINATOR '|'
//WITH row, apoc.util.md5([row.sr_system_id +'_'+ row.sr_supplier_id]) as sup_hid
//       WHERE row.sr_supplier_id IS NOT NULL
//WITH row, sup_hid
//       MATCH(s:SupplierID{hid:sup_hid})
//       SET
//              s.ordervolumemeur = toFloat(row.order_volume_eur) / (1000000),
//              s.invoicevolumemeur = toFloat(row.invoice_volume_eur) / (1000000)

