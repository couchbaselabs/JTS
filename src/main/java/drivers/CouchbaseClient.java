package main.java.drivers;

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
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import main.java.properties.TestProperties;


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
    private int connectTimeout = 10000;
    private int socketTimeout = 10000;

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

        switch (settings.get(settings.TESTSPEC_QUERY_TYPE)) {
            case TestProperties.CONSTANT_QUERY_TYPE_TERM:
                queryList = generateTermQueries(terms, limit, indexName, fieldName);
                break;
        }

        if ((queryList == null) || (queryList.size() == 0)) {
            throw new Exception("Query list is empty!");
        }

        queries  = queryList.stream().toArray(SearchQuery[]::new);
        totalQueries = queries.length;
    }



    private List<SearchQuery> generateTermQueries(String[][] terms, int limit, String indexName, String fieldName){
        List<SearchQuery> queryList = new ArrayList<>();
        int size = terms.length;

        for (int i = 0; i< size; i++) {
            int lineSize = terms[i].length;
            if (lineSize > 0) {
                String term = terms[i][0];
                SearchQuery query = buildTermQuery(term, limit, indexName, fieldName);
                queryList.add(query);
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
        bucket.query(queryToRun);
        long en = System.nanoTime();
        return (float) (en - st) / 1000000;
    }

    public String queryAndResponse(){
        SearchQueryResult res =  bucket.query(queries[rand.nextInt(totalQueries)]);
        return res.toString();
   }



   // Query builders

    private SearchQuery buildTermQuery(String term, int limit, String indexName, String fieldName) {
        return new SearchQuery(indexName, SearchQuery.term(term)).limit(limit).fields(fieldName);
    }




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



