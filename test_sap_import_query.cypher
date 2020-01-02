//Create or update all nodes level by level starting with local suppliers->DUNS->NatDUNS->GmDuns and add their respective relations

//Create all nonexisting local suppliers with a unique id consisting of Source System and local ID
Load CSV with headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' as row fieldterminator ';' 
with row, apoc.util.md5([row.sr_system_id + '_' + row.sr_supplier_id]) As supplier_uid
    where (not supplier_uid is null) and (not row.sr_supplier_id = '')
        with row, supplier_uid
            merge (child:SUPPLIER{uid:supplier_uid}) 
                On create set 
                    child.systemID = row.sr_system_id,
                    child.duns = row.sr_supplier_id,
                    child.srUniqueID = row.sr_system_id + "_" + row.sr_supplier_id;

//Create all nonexisting DUNS level nodes with their DUNS-ID as unique identifier
Load CSV with headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' as row fieldterminator ';' 
with row, row.sr_supplier_duns_id As duns_id, 'IFRP' as origin
    where (not duns_id in ['#','','NDM999999','NOH999999']) and (not duns_id is null)
        merge (d:DUNS{uid:duns_id}) 
            On create set 
                d.origin = origin,
                d.duns = duns_id,
                d.dunsName = row.sr_supplier_duns_name;

//Create all nonexisting NATIONAL-DUNS level nodes with their DUNS-ID as unique identifier
//Also create DUNS level node if nonexisting
Load CSV with headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' as row fieldterminator ';' 
with row, row.sr_supplier_national_mother_duns_id As nat_duns_id, 'IFRP' as origin
    where (not nat_duns_id in ['#','','NDM999999','NOH999999']) and (not nat_duns_id is null)
        merge (n:NATDUNS{uid:nat_duns_id}) 
            On create set 
                n.origin = origin,
                n.duns = nat_duns_id,
                n.dunsName = row.sr_supplier_national_mother_duns_name
                WITH DISTINCT n
                    MERGE (d:DUNS{uid:n.duns})
                        ON CREATE SET
                            d.origin = n.origin,
                            d.duns = n.duns,
                            d.dunsName = n.dunsName;

//Create all nonexisting GLOBAL-MOTHER-DUNS level nodes with their DUNS-ID as unique identifier
//Also create DUNS and NATDUNS level node/s if nonexisting
Load CSV with headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' as row fieldterminator ';' 
with row, row.sr_supplier_global_mother_duns_id As gm_duns_id, 'IFRP' as origin
    where (not gm_duns_id in ['#','','NDM999999','NOH999999']) and (not gm_duns_id is null)
        merge (g:GLOBALDUNS{uid:gm_duns_id}) 
            On create set 
                g.origin = origin,
                g.duns = gm_duns_id,
                g.dunsName = row.sr_supplier_global_mother_duns_name
                WITH DISTINCT g
                    MERGE (n:NATDUNS{uid:g.duns})
                        ON CREATE SET
                            n.origin = g.origin,
                            n.duns = g.duns,
                            n.dunsName = g.dunsName
                            WITH DISTINCT n
                                MERGE (d:DUNS{uid:n.duns})
                                    ON CREATE SET
                                        d.origin = n.origin,
                                        d.duns = n.duns,
                                        d.dunsName = n.dunsName;


//----------------------------------------------------------------------------------------------------------------
// CREATE THE RELATIONSHIPS
//local suppliers to DUNS level
Load CSV with headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' as row fieldterminator ';' 
with row, apoc.util.md5([row.sr_system_id + '_' + row.sr_supplier_id]) As supplier_uid
    MATCH (child:SUPPLIER{uid:supplier_uid})
    WHERE (not supplier_uid is null) and (not row.sr_supplier_id = '')
        WITH row, child, row.sr_supplier_duns_id AS duns_id
            MATCH (father:DUNS{duns:duns_id})
            WHERE (NOT duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT duns_id IS NULL)
                WITH DISTINCT (child) as child, father, row
                    WITH DISTINCT(father) as father, child, row
                    MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
                        SET 
                            r.validation_level = row.source,
                            r.update_date = row.modification_date;                       

       
// duns -> natduns
Load CSV with headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' as row fieldterminator ';' 
WITH row, row.sr_supplier_duns_id as duns_id
    MATCH (child:DUNS{duns:duns_id})
    WHERE (NOT duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT duns_id IS NULL)
        WITH row, child, row.sr_supplier_national_mother_duns_id as nat_duns_id
            MATCH (father:NATDUNS{duns:nat_duns_id})
            WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL) 
                WITH DISTINCT (child) as child, father, row
                    WITH DISTINCT(father) as father, child, row
                    MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
                        SET 
                            r.validation_level = row.source,
                            r.update_date = row.modification_date;
// nat duns <- self duns
Load CSV with headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' as row fieldterminator ';' 
WITH row, row.sr_supplier_national_mother_duns_id as nat_duns_id
    MATCH (father:NATDUNS{duns:nat_duns_id})
    WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL) 
        WITH DISTINCT(father) as father
            MATCH (child:DUNS{duns:father.duns})
                WITH father, child
                MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
                    SET 
                    //TODO: Change these accordingly!
                        r.validation_level = 'PYD', //arbitrary values: recommend current date and PYD level verification
                        r.update_date = '2020-01-02'; 


// natduns -> gmduns
Load CSV with headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' as row fieldterminator ';' 
WITH row, row.sr_supplier_national_mother_duns_id  as nat_duns_id
    MATCH (child:NATDUNS{duns:nat_duns_id})
    WHERE (NOT nat_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT nat_duns_id IS NULL)
        WITH row, child, row.sr_supplier_global_mother_duns_id as gm_duns_id
            MATCH (father:GLOBALDUNS{duns:gm_duns_id})
            WHERE (NOT gm_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT gm_duns_id IS NULL)
                WITH DISTINCT (child) as child, father, row
                    WITH DISTINCT(father) as father, child, row
                    MERGE (child)-[r:BELONGS{origin:"IFRP"}]->(father)
                        SET 
                            r.validation_level = row.source,
                            r.update_date = row.modification_date;
// gm duns <- self nat duns <- self duns 
Load CSV with headers from 'https://raw.githubusercontent.com/KevinReier/Neo4jSandbox/master/test_sap_export.csv' as row fieldterminator ';' 
WITH row, row.sr_supplier_global_mother_duns_id as gm_duns_id
    MATCH (father:GLOBALDUNS{duns:gm_duns_id})
    WHERE (NOT gm_duns_id  IN [ '#', '','NDM999999', 'NOH999999'] ) AND (NOT gm_duns_id IS NULL)
        WITH DISTINCT(father) as father
            MATCH (child:NATDUNS{duns:father.duns})
                WITH father, child
                    MERGE (child)-[r:BELONGS{source:"IFRP"}]->(father)
                        SET 
                            //TODO: Change these accordingly!
                            r.validation_level = 'PYD', //arbitrary values: recommend current date and PYD level verification
                            r.update_date = '2020-01-02'
        WITH DISTINCT(child) as father
            MATCH (child:DUNS{duns:father.duns})
                WITH father, child
                    MERGE (child)-[r:BELONGS{source:"IFRP"}]->(father)
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

