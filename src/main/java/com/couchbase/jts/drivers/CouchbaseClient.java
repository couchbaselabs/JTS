package com.couchbase.jts.drivers;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.env.resources.IoPoolShutdownHook;
import com.couchbase.client.core.metrics.DefaultLatencyMetricsCollectorConfig;
import com.couchbase.client.core.metrics.DefaultMetricsCollectorConfig;
import com.couchbase.client.core.metrics.LatencyMetricsCollectorConfig;
import com.couchbase.client.core.metrics.MetricsCollectorConfig;
import com.couchbase.client.deps.io.netty.channel.DefaultSelectStrategyFactory;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.SelectStrategy;
import com.couchbase.client.deps.io.netty.channel.SelectStrategyFactory;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.deps.io.netty.util.IntSupplier;
import com.couchbase.client.deps.io.netty.util.concurrent.DefaultThreadFactory;
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


import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by oleksandr.gyryk on 10/3/17.
 */


public class  CouchbaseClient extends Client{

    private static volatile CouchbaseEnvironment env = null;
    private static final Object INIT_COORDINATOR = new Object();

    private int networkMetricsInterval = 0;
    private int runtimeMetricsInterval = 0;
    private int queryEndpoints = 1;
    private int kvEndpoints = 1;
    private int boost = 3;
    private boolean epoll = false;
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
            synchronized (INIT_COORDINATOR) {
                if (env == null) {

                    LatencyMetricsCollectorConfig latencyConfig = networkMetricsInterval <= 0
                            ? DefaultLatencyMetricsCollectorConfig.disabled()
                            : DefaultLatencyMetricsCollectorConfig
                            .builder()
                            .emitFrequency(networkMetricsInterval)
                            .emitFrequencyUnit(TimeUnit.SECONDS)
                            .build();

                    MetricsCollectorConfig runtimeConfig = runtimeMetricsInterval <= 0
                            ? DefaultMetricsCollectorConfig.disabled()
                            : DefaultMetricsCollectorConfig.create(runtimeMetricsInterval, TimeUnit.SECONDS);

                    DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment
                            .builder()
                            .queryEndpoints(queryEndpoints)
                            .callbacksOnIoPool(true)
                            .runtimeMetricsCollectorConfig(runtimeConfig)
                            .networkLatencyMetricsCollectorConfig(latencyConfig)
                            .socketConnectTimeout(socketTimeout)
                            .connectTimeout(connectTimeout)
                            .kvTimeout(kvTimeout)
                            .kvEndpoints(kvEndpoints);

                    // Tune boosting and epoll based on settings
                    SelectStrategyFactory factory = boost > 0 ?
                            new BackoffSelectStrategyFactory() : DefaultSelectStrategyFactory.INSTANCE;

                    int poolSize = boost > 0 ? boost : Integer.parseInt(
                            System.getProperty("com.couchbase.ioPoolSize",
                                    Integer.toString(DefaultCoreEnvironment.IO_POOL_SIZE))
                    );
                    ThreadFactory threadFactory = new DefaultThreadFactory("cb-io", true);

                    EventLoopGroup group = epoll ? new EpollEventLoopGroup(poolSize, threadFactory, factory)
                            : new NioEventLoopGroup(poolSize, threadFactory, SelectorProvider.provider(), factory);
                    builder.ioPool(group, new IoPoolShutdownHook(group));

                    env = builder.build();
                }
            }
            cluster = CouchbaseCluster.create(env, getProp(TestProperties.CBSPEC_SERVER));
            cluster.authenticate(getProp(TestProperties.CBSPEC_USER), getProp(TestProperties.CBSPEC_PASSWORD));
            bucket = cluster.openBucket(getProp(TestProperties.CBSPEC_CBBUCKET));
        } catch (Exception ex) {
            throw new Exception("Could not connect to Couchbase Bucket.", ex);
        }
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
                case TestProperties.CONSTANT_QUERY_TYPE_GEO_RADIUS:
                	return buildGeoRadiusQuery(terms,limit,indexName,fieldName, "5mi");

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
    
    private SearchQuery buildGeoRadiusQuery(String[] terms,int limit,String indexName,String feildName, String dist)
    {
    	//double locationLon, double locationLat, String distance
    	double locationLon= Double.parseDouble(terms[0]) ;
    	double locationLat = Double.parseDouble(terms[1]);
    	String distance = dist;
    	GeoDistanceQuery geoRad = SearchQuery.geoDistance(locationLon, locationLat, distance);
    	return new SearchQuery(indexName,geoRad).limit(limit);
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

    // ------------
    class BackoffSelectStrategyFactory implements SelectStrategyFactory {
        @Override
        public SelectStrategy newSelectStrategy() {
            return new BackoffSelectStrategy();
        }
    }


    class BackoffSelectStrategy implements SelectStrategy {

        private int counter = 0;

        @Override
        public int calculateStrategy(final IntSupplier supplier, final boolean hasTasks) throws Exception {
            int selectNowResult = supplier.get();
            if (hasTasks || selectNowResult != 0) {
                counter = 0;
                return selectNowResult;
            }
            counter++;

            if (counter > 2000) {
                LockSupport.parkNanos(1);
            } else if (counter > 3000) {
                Thread.yield();
            } else if (counter > 4000) {
                LockSupport.parkNanos(1000);
            } else if (counter > 5000) {
                // defer to blocking select
                counter = 0;
                return SelectStrategy.SELECT;
            }

            return SelectStrategy.CONTINUE;
        }
    }




}
