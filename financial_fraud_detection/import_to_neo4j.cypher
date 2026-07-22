
// ---- Creating Constraints
CREATE CONSTRAINT Person_personId IF NOT EXISTS FOR (p:Person) REQUIRE p.personId IS UNIQUE;
CREATE CONSTRAINT Company_companyId IF NOT EXISTS FOR (c:Company) REQUIRE c.companyId IS UNIQUE;
CREATE CONSTRAINT Account_accountId IF NOT EXISTS FOR (a:Account) REQUIRE a.accountId IS UNIQUE;
CREATE CONSTRAINT Transaction_transactionId IF NOT EXISTS FOR (t:Transaction) REQUIRE t.transactionId IS UNIQUE;

// ---- 1. Persons ----
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vinod-nageshwar/neo4j-projects/refs/heads/main/financial_fraud_detection/persons.csv' AS row
MERGE (p:Person {personId: row.personId})
SET p.name = row.name,
    p.dob = date(row.dob),
    p.nationality = row.nationality,
    p.occupation = row.occupation,
    p.isPEP = toBoolean(row.isPEP);

// ---- 2. Companies ----
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vinod-nageshwar/neo4j-projects/refs/heads/main/financial_fraud_detection/companies.csv' AS row
MERGE (c:Company {companyId: row.companyId})
SET c.name = row.name,
    c.incorporationCountry = row.incorporationCountry,
    c.incorporationDate = date(row.incorporationDate),
    c.industry = row.industry,
    c.isShell = toBoolean(row.isShell);

// ---- 3. Accounts ----
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vinod-nageshwar/neo4j-projects/refs/heads/main/financial_fraud_detection/accounts.csv' AS row
MERGE (a:Account {accountId: row.accountId})
SET a.bank = row.bank,
    a.accountType = row.accountType,
    a.country = row.country,
    a.openedDate = date(row.openedDate),
    a.riskScore = toFloat(row.riskScore);

// ---- 4. Ownership: Person/Company -[:OWNS]-> Account, and beneficial ownership of Company ----
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vinod-nageshwar/neo4j-projects/refs/heads/main/financial_fraud_detection/ownership.csv' AS row
CALL (row) {
  WITH row WHERE row.accountId IS NOT NULL AND row.accountId <> ''
  MATCH (a:Account {accountId: row.accountId})
  WITH row, a
  CALL (row, a) {
    WITH row, a WHERE row.ownerType = 'Person'
    MATCH (p:Person {personId: row.ownerId})
    MERGE (p)-[r:OWNS]->(a)
    SET r.role = row.role, r.ownershipPct = toInteger(row.ownershipPct)
  }
  CALL (row, a) {
    WITH row, a WHERE row.ownerType = 'Company'
    MATCH (c:Company {companyId: row.ownerId})
    MERGE (c)-[r:OWNS]->(a)
    SET r.role = row.role, r.ownershipPct = toInteger(row.ownershipPct)
  }
}
CALL (row) {
  WITH row WHERE row.role = 'BENEFICIAL_OWNER' AND row.companyId IS NOT NULL AND row.companyId <> ''
  MATCH (p:Person {personId: row.ownerId})
  MATCH (c:Company {companyId: row.companyId})
  MERGE (p)-[r:BENEFICIALLY_OWNS]->(c)
  SET r.ownershipPct = toInteger(row.ownershipPct)
}
RETURN count(*);

// ---- 5. Transactions: modeled as (Account)-[:SENT]->(Transaction)-[:TO]->(Account) ----
// This lets each transaction carry its own properties and still supports
// fast path/cycle queries between accounts.
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vinod-nageshwar/neo4j-projects/refs/heads/main/financial_fraud_detection/transactions.csv' AS row
MATCH (src:Account {accountId: row.sourceAccount})
MATCH (dst:Account {accountId: row.targetAccount})
CREATE (t:Transaction {
  transactionId: row.transactionId,
  amount: toFloat(row.amount),
  currency: row.currency,
  timestamp: datetime(row.timestamp),
  transactionType: row.transactionType,
  channel: row.channel,
  isSuspicious: toBoolean(row.isSuspicious),
  pattern: row.pattern
})
CREATE (src)-[:SENT]->(t)
CREATE (t)-[:TO]->(dst);

// ---- Optional: a direct Account->Account TRANSFERRED edge for fast graph algorithms
// (GDS centrality/community detection prefer simple relationships over the
// Transaction-node model above)
MATCH (src:Account)-[:SENT]->(t:Transaction)-[:TO]->(dst:Account)
MERGE (src)-[r:TRANSFERRED]->(dst)
ON CREATE SET r.txCount = 1, r.totalAmount = t.amount, r.hasSuspicious = t.isSuspicious
ON MATCH SET r.txCount = r.txCount + 1,
             r.totalAmount = r.totalAmount + t.amount,
             r.hasSuspicious = r.hasSuspicious OR t.isSuspicious;