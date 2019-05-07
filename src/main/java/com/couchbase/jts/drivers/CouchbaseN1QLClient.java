package com.couchbase.jts.drivers;


import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.jts.properties.TestProperties;

import java.util.ArrayList;
import java.util.List;

public class CouchbaseN1QLClient extends CouchbaseClient {

    private N1qlQuery[] queries;

    public CouchbaseN1QLClient(TestProperties workload) throws Exception{
        super(workload);
        connect();
        generateQueries();
    }


    private void generateQueries() throws Exception {
        String[][] terms = importTerms();
        int limit = Integer.parseInt(settings.get(TestProperties.TESTSPEC_QUERY_LIMIT));
        String indexName = settings.get(TestProperties.CBSPEC_INDEX_NAME);
        String fieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
        List<N1qlQuery> queryList= null;

        queryList = generateN1QLQueries(terms, limit, indexName, fieldName);

        if ((queryList == null) || (queryList.size() == 0)) {
            throw new Exception("Query list is empty!");
        }

        queries  = queryList.stream().toArray(N1qlQuery[]::new);
        totalQueries = queries.length;
    }


    private List<N1qlQuery> generateN1QLQueries(String[][] terms, int limit, String indexName, String fieldName)
            throws IllegalArgumentException {
        List<N1qlQuery> queryList = new ArrayList<>();
        int size = terms.length;

        for (int i = 0; i< size; i++) {
            int lineSize = terms[i].length;
            if (lineSize > 0) {
                try {
                    N1qlQuery query = buildN1QLQuery(terms[i], limit, indexName, fieldName);
                    queryList.add(query);
                } catch (IndexOutOfBoundsException ex) {
                    continue;
                }
            }
        }
        return queryList;
    }


    private N1qlQuery buildN1QLQuery(String[] terms, int limit, String indexName, String fieldName) {

        StringBuilder queryBulder = new StringBuilder();
        queryBulder.append("SELECT meta().id FROM `bucket-1` WHERE SEARCH(`");
        queryBulder.append(indexName);
        queryBulder.append("`, '");
        queryBulder.append(fieldName);
        queryBulder.append(":");
        queryBulder.append(terms[0]);
        queryBulder.append("') limit ");
        queryBulder.append(limit);

        return N1qlQuery.simple(queryBulder.toString());
    }

    @Override
    public float queryAndLatency() {
        N1qlQuery queryToRun = queries[rand.nextInt(totalQueries)];
        long st = System.nanoTime();
        N1qlQueryResult queryResult = bucket.query(queryToRun);
        long en = System.nanoTime();
        float latency = (float) (en - st) / 1000000;

        if ((queryResult != null) && (queryResult.finalSuccess())) { return latency; }
        fileError(queryResult.toString());
        return 0;
    }

    @Override
    public String queryDebug(){
        N1qlQuery queryToRun = queries[rand.nextInt(totalQueries)];
        N1qlQueryResult queryResult = bucket.query(queryToRun);
        StringBuilder resStr = new StringBuilder();
        resStr.append(queryResult.toString());
        for (N1qlQueryRow row : queryResult){
            resStr.append(row.toString());
            resStr.append('\n');
        }
        return resStr.toString();
    }

    @Override
    public void query() {
        bucket.query(queries[rand.nextInt(totalQueries)]);
    }

    @Override
    public Boolean queryAndSuccess(){
        return bucket.query(queries[rand.nextInt(totalQueries)]).finalSuccess();
    }

}
