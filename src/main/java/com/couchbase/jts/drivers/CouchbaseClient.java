package com.couchbase.jts.drivers;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;

import com.couchbase.jts.properties.TestProperties;

import java.util.List;
import java.util.Random;


public class  CouchbaseClient extends Client{

    private static volatile CouchbaseEnvironment env = null;

    private int kvTimeout = 10000;
    private int connectTimeout = 100000;
    private int socketTimeout = 100000;

    private Cluster cluster;
    private Bucket bucket;
    private SearchQuery[] queries;

    private Random rand = new Random();
    private int totalQueries = 0;
    private SearchQuery queryToRun;

    private JsonDocument replacementDocumentBuffer = null;
    private CouchbaseQueryBuilder queryBuilder = new CouchbaseQueryBuilder(fieldsMap, valuesMap, settings);


    public CouchbaseClient(TestProperties workload) throws Exception{
        super(workload);
        connect();
        generateQueries();
    }

    private void connect() throws Exception{

        try {

            DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment
                    .builder()
                    .callbacksOnIoPool(true)
                    .socketConnectTimeout(socketTimeout)
                    .connectTimeout(connectTimeout)
                    .kvTimeout(kvTimeout);

            env = builder.build();

        } catch (Exception ex) {
            throw new Exception("Could not connect to Couchbase Bucket.", ex);
        }

        cluster = CouchbaseCluster.create(env, getProp(TestProperties.CBSPEC_SERVER));
        cluster.authenticate(getProp(TestProperties.CBSPEC_USER), getProp(TestProperties.CBSPEC_PASSWORD));
        bucket = cluster.openBucket(getProp(TestProperties.CBSPEC_CBBUCKET));

    }

    private void generateQueries() throws Exception {

        int limit = Integer.parseInt(settings.get(TestProperties.TESTSPEC_QUERY_LIMIT));
        String indexName = settings.get(TestProperties.CBSPEC_INDEX_NAME);

        List<SearchQuery> queryList= null;

        queryList = generateQueryList(limit, indexName);

        if ((queryList == null) || (queryList.size() == 0)) {
            throw new Exception("Query list is empty!");
        }

        queries  = queryList.stream().toArray(SearchQuery[]::new);
        totalQueries = queries.length;
    }


    private List<SearchQuery> generateQueryList(int limit, String indexName)
                    throws IllegalArgumentException, IndexOutOfBoundsException {

            switch (settings.get(settings.TESTSPEC_QUERY_TYPE)) {
                case TestProperties.CONSTANT_QUERY_TYPE_TERM:
                    return queryBuilder.buildTermQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_AND:
                    return queryBuilder.buildAndQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_OR:
                    return queryBuilder.buildOrQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_AND_OR_OR:
                    return queryBuilder.buildAndOrOrQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_FUZZY:
                    return queryBuilder.buildFuzzyQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_PHRASE:
                    return queryBuilder.buildPhraseQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_PREFIX:
                    return queryBuilder.buildPrefixQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_WILDCARD:
                    return queryBuilder.buildWildcardQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_FACET:
                    return queryBuilder.buildFacetQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_NUMERIC:
                    return queryBuilder.buildNumericQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_1BT:
                    return queryBuilder.build1BTermQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_1BTA:
                    return queryBuilder.build1BTermArrayQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_1BD:
                    return queryBuilder.build1BOrQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_1BC:
                    return queryBuilder.build1BAndQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_1BDCDCD:
                    return queryBuilder.build1BDCDCDQueries(limit, indexName);
                case TestProperties.CONSTANT_QUERY_TYPE_1BDDDCDDD:
                    return queryBuilder.build1BDDDCDDDQueries(limit, indexName);
            }
            throw new IllegalArgumentException("Couchbase query builder: unexpected query type - "
                    + settings.get(settings.TESTSPEC_QUERY_TYPE));
        }





    private String getProp(String name) {
        return getWorkload().get(name);
    }

    public float queryAndLatency() {
        queryToRun = queries[rand.nextInt(totalQueries)];
        long st = System.nanoTime();
        SearchQueryResult res = bucket.query(queryToRun);
        long en = System.nanoTime();
        float latency = (float) (en - st) / 1000000;

        if ((res != null) && (res.status().isSuccess()) && (res.metrics().totalHits() > 0)) { return latency; }
        fileError(res.toString());
        return 0;
    }


    public String queryDebug(){
        return bucket.query(queries[rand.nextInt(totalQueries)]).toString();
    }

    public void query() {
        bucket.query(queries[rand.nextInt(totalQueries)]);
    }

    public Boolean queryAndSuccess(){
        return bucket.query(queries[rand.nextInt(totalQueries)]).status().isSuccess();
    }

    public void kv() {
        replaceRandomDoc();
    }

    private void replaceRandomDoc() {
        long totalDocs = Long.parseLong(getWorkload().get(TestProperties.TESTSPEC_TOTAL_DOCS));
        long docIdLong = Math.abs(rand.nextLong() % totalDocs);
        String docIdHex = Long.toHexString(docIdLong);
        JsonDocument doc = bucket.get(docIdHex);

        if (replacementDocumentBuffer != null) {
            bucket.replace(JsonDocument.create(docIdHex, replacementDocumentBuffer.content()));
        }

        replacementDocumentBuffer = doc;
    }

    private void fileError(String err) {
        System.out.println(err);
    }



}



