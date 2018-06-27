# Neo4j & BigQuery

Blog post coming soon...

## Installation

1. Build the fat jar using `gradle clean shadowJar` and copy to your `$NEO4J_HOME/plugins` folder.
2. Set the Project ID and path to credentials file in `neo4j.conf`
    ```
    bigquery.project_id=neo4j-bigquery-test
    bigquery.key_file=/path/to/credentials.json
    ```  
3. Restart Neo4j

## Usage

> `bigQuery.run(query [, useLegacySql])`

Run the `bigQuery.run` procedure with a query and optionally set the `useLegacySql` argument to true.

```cypher
CALL bigQuery.run('
    SELECT id, title 
    FROM `bigquery-public-data.stackoverflow.posts_questions` 
    LIMIT 10
') YIELD row

```

**Note:**  This library currently doesn't handle nested (repeated) elements well. If your query contains nested elements, you will have to un-nest them in the SQL query using `UNNEST`.  
```sql
UNNEST (hits) as hits
```
*It's on the TODO list.*