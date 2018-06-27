package org.neo4j.bigquery;

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;

import static org.junit.Assert.assertEquals;

public class BigQueryProceduresTest
{


    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(BigQueryProcedures.class)
            .withConfig("bigquery.credentials", "/Users/adam/projects/bigquery/src/main/resources/service_account.json")
            .withConfig("bigquery.project_id", "neo4j-bigquery-test");

    @Test
    public void shouldReturnFlatResult() throws Throwable
    {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();

        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("CALL bigQuery.run('SELECT id, title FROM `bigquery-public-data.stackoverflow.posts_questions` LIMIT 10') YIELD row");

            Map<String, Object> next = res.next();
            System.out.println( next );

        }
    }

    @Test
    public void shouldReturnNestedResult() throws Throwable
    {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();

        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("CALL bigQuery.run('SELECT visitorId, visitNumber, visitId, visitStartTime, " +
                    "  hits FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` LIMIT 1') YIELD row");

            Map<String, Object> next = res.next();

            System.out.println("--");
            System.out.println("--");
            System.out.println("--");
            System.out.println("--");
            System.out.println("vv");
            System.out.println( next.get("row") );

        }
    }


}
