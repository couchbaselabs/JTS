package com.couchbase.jts.drivers;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.queries.*;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;

import com.couchbase.jts.properties.TestProperties;

import java.util.ArrayList;
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
        String[][] terms = importTerms();
        int limit = Integer.parseInt(settings.get(TestProperties.TESTSPEC_QUERY_LIMIT));
        String indexName = settings.get(TestProperties.CBSPEC_INDEX_NAME);
        String fieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
        List<SearchQuery> queryList= null;

        queryList = generateTermQueries(terms, limit, indexName, fieldName);

        if ((queryList == null) || (queryList.size() == 0)) {
            throw new Exception("Query list is empty!");
        }

        queries  = queryList.stream().toArray(SearchQuery[]::new);
        totalQueries = queries.length;
    }

    private List<SearchQuery> generateTermQueries(String[][] terms, int limit, String indexName, String fieldName)
            throws IllegalArgumentException {
        List<SearchQuery> queryList = new ArrayList<>();
        int size = terms.length;

        for (int i = 0; i< size; i++) {
            int lineSize = terms[i].length;
            if (lineSize > 0) {
                try {
                    SearchQuery query = buildQuery(terms[i], limit, indexName, fieldName);
                    queryList.add(query);
                } catch (IndexOutOfBoundsException ex) {
                    continue;
                }
            }
        }
        return queryList;
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

    public void mutateRandomDoc() {
        long totalDocs = Long.parseLong(getWorkload().get(TestProperties.TESTSPEC_TOTAL_DOCS));
        long docIdLong = Math.abs(rand.nextLong() % totalDocs);
        String docIdHex = Long.toHexString(docIdLong);
        String originFieldName = getWorkload().get(TestProperties.TESTSPEC_QUERY_FIELD);
        String replaceFieldName = getWorkload().get(TestProperties.TESTSPEC_MUTATION_FIELD);

        JsonDocument doc = bucket.get(docIdHex);
        Object origin = doc.content().get(originFieldName);
        Object replace = doc.content().get(replaceFieldName);
        doc.content().put(originFieldName, replace);
        doc.content().put(replaceFieldName, origin);
        bucket.upsert(doc);
    }

    private void fileError(String err) {
        System.out.println(err);
    }


   // Query builders
    private SearchQuery buildQuery(String[] terms, int limit, String indexName, String fieldName)
            throws IllegalArgumentException, IndexOutOfBoundsException {

            switch (settings.get(settings.TESTSPEC_QUERY_TYPE)) {
                case TestProperties.CONSTANT_QUERY_TYPE_TERM:
                    return buildTermQuery(terms, limit, indexName, fieldName);
                case TestProperties.CONSTANT_QUERY_TYPE_AND:
                    return buildAndQuery(terms, limit, indexName, fieldName);
                case TestProperties.CONSTANT_QUERY_TYPE_OR:
                    return buildOrQuery(terms, limit, indexName, fieldName);
                case TestProperties.CONSTANT_QUERY_TYPE_AND_OR_OR:
                    return buildAndOrOrQuery(terms, limit, indexName, fieldName);
                case TestProperties.CONSTANT_QUERY_TYPE_FUZZY:
                    return buildFuzzyQuery(terms, limit, indexName, fieldName);
                case TestProperties.CONSTANT_QUERY_TYPE_PHRASE:
                    return buildPhraseQuery(terms, limit, indexName, fieldName);
                case TestProperties.CONSTANT_QUERY_TYPE_PREFIX:
                    return buildPrefixQuery(terms, limit, indexName, fieldName);
                case TestProperties.CONSTANT_QUERY_TYPE_WILDCARD:
                    return buildWildcardQuery(terms, limit, indexName, fieldName);
                case TestProperties.CONSTANT_QUERY_TYPE_FACET:
                    return buildFacetQuery(terms, limit, indexName, fieldName);
                case TestProperties.CONSTANT_QUERY_TYPE_NUMERIC:
                    return buildNumericQuery(terms, limit, indexName, fieldName);
            }
            throw new IllegalArgumentException("Couchbase query builder: unexpected query type - "
                    + settings.get(settings.TESTSPEC_QUERY_TYPE));
    }


    private SearchQuery buildTermQuery(String[] terms, int limit, String indexName, String fieldName) {
        return new SearchQuery(indexName, SearchQuery.term(terms[0]).field(fieldName)).limit(limit);
    }

    private SearchQuery buildAndQuery(String[] terms, int limit, String indexName, String fieldName) {
            TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
            TermQuery rt = SearchQuery.term(terms[1]).field(fieldName);
            ConjunctionQuery conjSQ = SearchQuery.conjuncts(lt, rt);
            return new SearchQuery(indexName, conjSQ).limit(limit);
    }

    private SearchQuery buildOrQuery(String[] terms, int limit, String indexName, String fieldName) {
        TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
        TermQuery rt = SearchQuery.term(terms[1]).field(fieldName);
        DisjunctionQuery disSQ = SearchQuery.disjuncts(lt, rt);
        return new SearchQuery(indexName, disSQ).limit(limit);
    }

    private SearchQuery buildAndOrOrQuery(String[] terms, int limit, String indexName, String fieldName) {
        TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
        TermQuery mt = SearchQuery.term(terms[1]).field(fieldName);
        TermQuery rt = SearchQuery.term(terms[2]).field(fieldName);
        DisjunctionQuery disSQ = SearchQuery.disjuncts(mt, rt);
        ConjunctionQuery conjSQ = SearchQuery.conjuncts(disSQ, lt);
        return new SearchQuery(indexName, conjSQ).limit(limit);
    }

    private SearchQuery buildFuzzyQuery(String[] terms, int limit, String indexName, String fieldName){
        return new SearchQuery(indexName, SearchQuery.term(terms[0]).field(fieldName).
                fuzziness(Integer.parseInt(terms[1]))).
                limit(limit);
    }

    private SearchQuery buildPhraseQuery(String[] terms, int limit, String indexName, String fieldName) {
        MatchPhraseQuery mphSQ = SearchQuery.matchPhrase(terms[0] + " " + terms[1]).field(fieldName);
        return new SearchQuery(indexName, mphSQ).limit(limit);
    }

    private SearchQuery buildPrefixQuery(String[] terms, int limit, String indexName, String fieldName) {
        PrefixQuery prefSQ = SearchQuery.prefix(terms[0]).field(fieldName);
        return new SearchQuery(indexName, prefSQ).limit(limit);
    }

    private SearchQuery buildWildcardQuery(String[] terms, int limit, String indexName, String fieldName)  {
        WildcardQuery wcSQ = SearchQuery.wildcard(terms[0]).field(fieldName);
        return new SearchQuery(indexName, wcSQ).limit(limit);

    }

    private SearchQuery buildFacetQuery(String[] terms, int limit, String indexName, String fieldName) {
        String[] dates = terms[1].split(":");

        TermQuery tSQ = SearchQuery.term(terms[0]).field(fieldName);
        SearchQuery resultQuery = new SearchQuery(indexName, tSQ)
                .addFacet("bydaterange", SearchFacet.date("date", limit)
                        .addRange("dateranges", dates[0], dates[1]))
                .limit(limit);
        return resultQuery;
    }

    private SearchQuery buildNumericQuery(String[] terms, int limit, String indexName, String fieldName) {
        String[] minmax = terms[0].split(":");

        NumericRangeQuery nrgSQ = SearchQuery.numericRange()
                .max(Double.parseDouble(minmax[0]), true)
                .min(Double.parseDouble(minmax[1]), true).field(fieldName);
        return new SearchQuery(indexName, nrgSQ).limit(limit);
    }
}



