MATCH path = (first:Account)-[:TRANSFERRED*1..6]->(last:Account)
WHERE NOT ()-[:TRANSFERRED]->(first) AND NOT (last)-[:TRANSFERRED]->()
RETURN path LIMIT 1
