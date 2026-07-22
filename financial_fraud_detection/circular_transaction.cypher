MATCH (a:Account)
WITH collect(a) AS accounts
CALL apoc.nodes.cycles(accounts, {relTypes: ['TRANSFERRED'], maxDepth: 6})
YIELD path
RETURN path LIMIT 2
