// CREATE THE NODES
//dunsid
LOAD CSV WITH HEADERS FROM 'file:///DNB_FINAL_Merged_Files_1812_1902_V2.csv' AS row FIELDTERMINATOR '|'
WITH row, apoc.util.md5(['DNB_'+ row.duns_no]) AS duns_hid, row.duns_no as dunsid, 'DNB_' AS prefix
        WHERE NOT dunsid  IN [ '#', '','NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH row, duns_hid, dunsid, prefix
        MERGE (d:Duns{hid:duns_hid})
               SET
                       d.mdm_sysid = prefix,
                       d.supplierlocalid = dunsid,
                       d.supplieruniqueid = prefix + dunsid,
                       d.duns = dunsid,
                       d.name = coalesce(row.name + ' ' + row.snd_name, row.name),
                       d.street = coalesce(row.add + '  ' + row.add_snd, row.add),
                       d.city = row.city,
                       d.postalcode = row.post_code,
                       d.state = row.state,
                       d.registerid = row.nat_id,
                       d.countrycode = row.country_code,
                       d.update_date = '2019-02-18',
                       d.source = 'DNB';


//natdunsid and its duns
LOAD CSV WITH HEADERS FROM 'file:///DNB_FINAL_Merged_Files_1812_1902_V2.csv' AS row FIELDTERMINATOR '|'
WITH row, apoc.util.md5(['natDNB_'+ row.du_duns]) AS duns_hid, row.du_duns as dunsid, 'natDNB_' AS prefix
        WHERE NOT dunsid  IN [ '#','', 'NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH row, duns_hid, dunsid, prefix
        MERGE (n:NatDuns{hid:duns_hid})
        SET
                       n.mdm_sysid = prefix,
                       n.supplierlocalid = dunsid,
                       n.supplieruniqueid = prefix + dunsid,
                       n.duns = dunsid,
                       n.countrycode = row.du_country_code,
                       n.name = row.du_name,
                       n.street = row.du_add,
                       n.city= row.du_city,
                       n.postalcode = row.nat_postalcode,
                       n.state = row.du_state,
                       n.update_date = '2019-02-18',
                       n.source = 'DNB'               
WITH DISTINCT n
    MERGE (n2:Duns{hid: apoc.util.md5(['DNB_'+ n.supplierlocalid])})
               ON CREATE SET
                       n2.mdm_sysid = 'DNB',
                       n2.supplierlocalid = n.supplierlocalid,
                       n2.supplieruniqueid = 'DNB_'+ n.supplierlocalid,
                       n2.duns = n.supplierlocalid,
                       n2.name = n.name,
                       n2.countrycode = n.countrycode,
                       n2.street = n.street,
                       n2.city= n.city,
                       n2.postalcode = n.postalcode,
                       n2.state = n.state,
                       n2.update_date = '2019-02-18',
                       n2.source = 'DNB';
                       

// gmduns and natduns and duns
LOAD CSV WITH HEADERS FROM 'file:///DNB_FINAL_Merged_Files_1812_1902_V2.csv' AS row FIELDTERMINATOR '|'
WITH row, apoc.util.md5(['gmDNB_'+ row.gu_duns]) AS duns_hid, row.gu_duns as dunsid, 'gmDNB_' AS prefix
        WHERE NOT dunsid  IN [ '#', '', 'NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH row, duns_hid, dunsid, prefix
        MERGE (g:GmDuns{hid:duns_hid})
        SET
                       g.mdm_sysid = prefix,
                       g.supplierlocalid = dunsid,
                       g.supplieruniqueid = prefix + dunsid,
                       g.duns = dunsid,
                       g.countrycode = row.gu_country_code,
                       g.name = row.gu_name,
                       g.street = row.gu_add,
                       g.city = row.gu_city,
                       g.postalcode = row.gu_post_code,
                       g.state = row.gu_state,
                       g.update_date = '2019-02-18',
                       g.source = 'DNB'
WITH DISTINCT g
        MERGE (n:NatDuns{hid:apoc.util.md5(['natDNB_'+ g.supplierlocalid])})
        ON CREATE SET
               n.mdm_sysid = 'natDNB',
               n.supplierlocalid = g.supplierlocalid,
               n.supplieruniqueid = 'natDNB_'+ g.supplierlocalid,
               n.duns = g.supplierlocalid,
               n.name = g.name,
               n.countrycode = g.countrycode,
               n.street = g.street,
               n.city = g.city,
               n.postalcode = g.postalcode,
               n.state = g.state,
               n.update_date = '2019-02-18',
               n.source = 'DNB'       
WITH DISTINCT n
    MERGE (d:Duns{hid: apoc.util.md5(['DNB_'+ n.supplierlocalid])})
               ON CREATE SET
                       d.mdm_sysid = 'DNB',
                       d.supplierlocalid = n.supplierlocalid,
                       d.supplieruniqueid = 'DNB_'+ n.supplierlocalid,
                       d.duns = n.supplierlocalid,
                       d.name = n.name,
                       d.countrycode = n.countrycode,
                       d.street = n.street,
                       d.city = n.city,
                       d.postalcode = n.postalcode,
                       d.state = n.state,
                       d.update_date = '2019-02-18',
                       d.source = 'DNB';

                       
                       
// CREATE THE RELATIONSHIPS

        
// duns -> natduns
LOAD CSV WITH HEADERS FROM 'file:///DNB_FINAL_Merged_Files_1812_1902_V2.csv' AS row FIELDTERMINATOR '|'
WITH row, row.duns_no as dunsid
        MATCH (child:Duns{supplierlocalid:dunsid})
        WHERE NOT dunsid  IN [ '#','', 'NDM999999', 'NOH999999'] AND dunsid IS NOT NULL
WITH row, child, row.du_duns as dunsid
        MATCH (father:NatDuns{supplierlocalid:dunsid})
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
        MATCH (father:GmDuns{supplierlocalid:dunsid})
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
