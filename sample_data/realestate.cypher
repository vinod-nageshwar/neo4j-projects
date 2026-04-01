--Creating Constraints
CREATE CONSTRAINT area_name IF NOT EXISTS FOR (n:Area) REQUIRE n.name IS UNIQUE
CREATE CONSTRAINT locality_address IF NOT EXISTS FOR (n:Locality) REQUIRE n.address IS UNIQUE
CREATE CONSTRAINT project_name IF NOT EXISTS FOR (n:Project) REQUIRE n.name IS UNIQUE
CREATE CONSTRAINT promoter_name IF NOT EXISTS FOR (n:Promoter) REQUIRE n.name IS UNIQUE

--Cypher for Loading the data
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/vinod-nageshwar/neo4j-projects/refs/heads/main/sample_data/realestate.json') YIELD value RETURN value",
"MERGE (prj:Project {name: trim(value.project)})
SET prj.description = trim(value.description)
MERGE (prm:Promoter {name: trim(value.promoter)})
MERGE (prj)-[:PROMOTED_BY]->(prm)
MERGE (ly:Locality {address: trim(value.address)})
MERGE (prj)-[:LOCATED_IN]->(ly)
MERGE (ar:Area {name: trim(value.location)})
MERGE (ly)-[:IS_IN]->(ar)
WITH value,prj,ar
CALL apoc.spatial.geocodeOnce(value.location) YIELD location, latitude, longitude
SET ar.latitude = latitude,ar.longitude = longitude
WITH prj,value
UNWIND value.config as config
CREATE (prp:Property {bhk: toInteger(config.bhk)})   
SET prp.min_size_sq_ft = config.min_size_sq_ft,
   prp.max_size_sq_ft = config.max_size_sq_ft,	   
   prp.min_price = config.min_price,
   prp.max_price = config.max_price
MERGE (prp)-[:BELONGS_TO]->(prj)
",
{batchSize: 10, parallel: false, retries: 0})
