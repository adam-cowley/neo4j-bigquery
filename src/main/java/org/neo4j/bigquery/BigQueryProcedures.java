package org.neo4j.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class BigQueryProcedures
{

    @Context
    public GraphDatabaseAPI db;

    @Procedure( value = "bigQuery.run", mode = Mode.READ )
    @Description( "bigQuery.run(query [, useLegacySql]) | Run a SQL query against BigQuery" )
    public Stream<Row> runQuery(
            @Name( "query" ) String query,
            @Name( value = "useLegacySql", defaultValue = "false") Boolean useLegacySql
    )  {
        try
        {
            Job queryJob = createJob( query, useLegacySql );

            TableResult result = queryJob.getQueryResults();

            Schema schema = result.getSchema();
            FieldList fields = schema.getFields();

            List<Row> output = new ArrayList<>();

            for ( FieldValueList row : result.iterateAll() )
            {
                Map<String,Object> outputRow = toMap( row, fields );

                output.add( new Row( outputRow ) );
            }

            return output.stream();
        }
        catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }


    private BigQuery getService() {
        Map<String, String> params = db.getDependencyResolver().resolveDependency(Config.class).getRaw();

        String credentialsFile = params.get("bigquery.key_file");
        String projectId = params.get("bigquery.project_id");

        GoogleCredentials credentials;
        File credentialsPath = new File(credentialsFile);

        try ( FileInputStream serviceAccountStream = new FileInputStream(credentialsPath) ) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }
        catch ( Exception e) {
            throw new RuntimeException( "Could not load credentials file "+ credentialsFile );
        }

        return BigQueryOptions.newBuilder()
                .setProjectId( projectId )
                .setCredentials( credentials )
                .build()
                .getService();
    }

    private Job createJob(String query, Boolean useLegacySql) throws InterruptedException {
        BigQuery service = getService();

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                .setUseLegacySql(useLegacySql)
                .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());

        Job queryJob = service.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if ( queryJob == null ) {
            throw new RuntimeException("Job no longer exists");
        }
        else if ( queryJob.getStatus().getError() != null ) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        return queryJob;
    }

    private Map<String, Object> toMap( FieldValueList row, FieldList fields ) {
        Map<String, Object> output = new HashMap<>();

        for ( int i = 0; i < fields.size(); i++ ) {
            String key = fields.get(i).getName();

            Object value = row.get(i).getValue();

            output.put( key, value );
        }

        return output;
    }

    protected static class Row {
        public Object row;

        public Row(Object row) {
            this.row = row;
        }
    }
}
