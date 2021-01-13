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
import com.couchbase.client.java.search.queries.Coordinate;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.queries.*;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.jts.logger.GlobalStatusLogger;


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
	private GlobalStatusLogger logWriter = new GlobalStatusLogger();
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
    private N1qlQuery[] flexQueries;


    private Random rand = new Random();
    private int totalQueries = 0;
    private int flexTotalQueries = 0;
    private SearchQuery queryToRun;
    private N1qlQuery flexQueryToRun;
    private Boolean flexFlag ;


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
        List<N1qlQuery> flexQueryList = null;
        flexFlag = Boolean.parseBoolean(settings.get(TestProperties.TESTSPEC_FLEX));
        if(flexFlag ) {
        	flexQueryList = generateFlexQueries(terms,limit,indexName);
        	if ((flexQueryList == null) || (flexQueryList.size() == 0)) {
                throw new Exception("Flex query list is empty!");
            }
        	flexQueries = flexQueryList.stream().toArray(N1qlQuery[]::new);
        	flexTotalQueries = flexQueries.length;
        }
        else {
        	queryList = generateTermQueries(terms, limit, indexName, fieldName);
        	if ((queryList == null) || (queryList.size() == 0)) {
                throw new Exception("Query list is empty!");
            }
        	queries  = queryList.stream().toArray(SearchQuery[]::new);
        	totalQueries = queries.length;
        }

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

    private List<N1qlQuery> generateFlexQueries(String[][] terms, int limit, String indexName)
    		throws IllegalArgumentException {
    	List<N1qlQuery> queryList = new ArrayList<>();
    	int size = terms.length;
    	for (int i = 0; i<size; i++ ) {
    		int lineSize = terms[i].length;
    		if(lineSize >0) {
    			try {
    				N1qlQuery query = buildFlexQuery(terms[i],limit,indexName);
    				queryList.add(query);
    			}catch(IndexOutOfBoundsException ex) {
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
    	long st = System.nanoTime();
    	SearchQueryResult res = null;
    	N1qlQueryResult flexRes = null;
    	if(flexFlag) {
    		flexQueryToRun = flexQueries[rand.nextInt(flexTotalQueries)];
    		flexRes = bucket.query(flexQueryToRun);
    		logWriter.logMessage(String.valueOf(flexRes));
    	}
    	else {
    		queryToRun = queries[rand.nextInt(totalQueries)];
    		res = bucket.query(queryToRun);
    	}



        long en = System.nanoTime();
        float latency = (float) (en - st) / 1000000;
        if(flexFlag) {
        	if ( flexRes.parseSuccess() && flexRes.finalSuccess()){return latency; }
        	fileError(flexRes.toString());


        }else {
        	if ((res != null) && (res.status().isSuccess()) && (res.metrics().totalHits() > 0)) { return latency; }
        	fileError(res.toString());
        }

        return 0;
    }


    public String queryDebug(){
    	if(flexFlag) {
    		return bucket.query(flexQueries[rand.nextInt(flexTotalQueries)]).toString();
    	}else {
    		return bucket.query(queries[rand.nextInt(totalQueries)]).toString();
    	}

    }

    public void query() {
    	if (flexFlag) {
    		bucket.query(flexQueries[rand.nextInt(flexTotalQueries)]);
    	}else {
    		bucket.query(queries[rand.nextInt(totalQueries)]);
    	}

    }

    public Boolean queryAndSuccess(){

    	if(flexFlag) {
    	     N1qlQueryResult flexRes = bucket.query(flexQueries[rand.nextInt(flexTotalQueries)]);
    	     if ( flexRes.parseSuccess() && flexRes.finalSuccess()){return true;}
    	     return false;

    	}else{
    	    return bucket.query(queries[rand.nextInt(totalQueries)]).status().isSuccess();
    	}



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

    //FlexQueryBuilders
    private N1qlQuery buildFlexQuery(String[] terms, int limit,String indexName)
    		throws IllegalArgumentException{
    		switch(settings.get(settings.TESTSPEC_FLEX_QUERY_TYPE)) {
    		case TestProperties.CONSTANT_FLEX_QUERY_TYPE_ARRAY :
    			return buildComplexObjQuery(terms,limit, indexName);
    		case TestProperties.CONSTANT_FLEX_QUERY_TYPE_MIXED1:
    			return buildMixedQuery1(terms,limit,indexName);
    		case TestProperties.CONSTANT_FLEX_QUERY_TYPE_MIXED2:
    			return buildMixedQuery2(terms,limit,indexName);
    		}
    		throw new IllegalArgumentException("Couchbase query builder: unexpected flex query type.");
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
                	return buildGeoRadiusQuery(terms,limit,indexName,fieldName,settings.get(settings.TESTSPEC_GEO_DISTANCE));
                case TestProperties.CONSTANT_QUERY_TYPE_GEO_BOX:
                	double latHeight = Double.parseDouble(settings.get(settings.TESTSPEC_GEO_LAT_HEIGHT));
                	double lonWidth = Double.parseDouble(settings.get(settings.TESTSPEC_GEO_LON_WIDTH));
                	return buildGeoBoundingBoxQuery(terms,limit,indexName,fieldName , latHeight,lonWidth);
                case TestProperties.CONSTANT_QUERY_TYPE_GEO_POLYGON:
                	return buildGeoPolygonQuery(terms,limit,indexName,fieldName);


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
    	GeoDistanceQuery geoRad = SearchQuery.geoDistance(locationLon, locationLat, distance).field(feildName);
    	return new SearchQuery(indexName,geoRad).limit(limit);
    }
    private SearchQuery buildGeoBoundingBoxQuery(String[] terms,int limit,String indexName,String feildName , double latHeight, double lonWidth)
    {
    	//double topLeftLon, double topLeftLat,double bottomRightLon, double bottomRightLat
    	double topLeftLon= Double.parseDouble(terms[0]) ;
    	double topLeftLat = Double.parseDouble(terms[1]);
    	double bottomRightLon= topLeftLon +lonWidth ;
    	double bottomRightLat = topLeftLat - latHeight;
    	GeoBoundingBoxQuery geoRad = SearchQuery.geoBoundingBox(topLeftLon,topLeftLat, bottomRightLon,bottomRightLat).field(feildName);
    	return new SearchQuery(indexName,geoRad).limit(limit);
    }
    private SearchQuery buildGeoPolygonQuery(String[] terms, int limit,String indexName,String fieldName )
    {
    	List<Coordinate> listOfPts =  new ArrayList<Coordinate>();
    	for(int i = 0; i <terms.length;i = i+2)
    	{
    		double lon = Double.parseDouble(terms[i]);
    		double lat = Double.parseDouble(terms[i+1]);
    		Coordinate coord = Coordinate.ofLonLat(lon,lat);
    		listOfPts.add(coord);
    	}

    	GeoPolygonQuery geoPol = SearchQuery.geoPolygon(listOfPts).field(fieldName);
    	return new SearchQuery(indexName,geoPol).limit(limit);
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

    private N1qlQuery buildComplexObjQuery(String[] terms, int limit, String indexName) {

    	String query ="SELECT devices, company_name, first_name "
    	        + "FROM `bucket-1` USE INDEX( perf_fts_index USING FTS) "
    			+ "WHERE (((ANY c IN children SATISFIES c.gender = \"M\"  AND c.age <=8 AND c.first_name = \"Aaron\" END) "
    			+ "OR (ANY num in devices SATISFIES num >= \"070842-712\" AND num<=\"070875-000\" END) ) ) "
    			+ "AND ((ANY num in devices SATISFIES num >= \"060842-712\" AND num<=\"060843-712\" END) "
    			+ "OR  (ANY c in children SATISFIES (c.first_name =\"Tyra\" or c.first_name =\"Aaron\") "
    			+ "AND c.gender = \"F\" AND c.age>=10 AND c.age<=13 END))OR(ANY c IN children SATISFIES c.gender = \"F\" "
    			+ "AND c.age <=5 AND (first_name=\"Sienna\" OR first_name= \"Pattie\" ) END )";
    	return N1qlQuery.simple(query);

    }

    private N1qlQuery buildMixedQuery1(String[] terms, int limit , String indexName) {

    	String query = "select first_name , routing_number, city , country, age "
    			+ "from `bucket-1` USE index (using FTS) "
    			+"where ((routing_number>=1011 AND routing_number<=1020) "
    			+"OR (address.city =\"Schoenview\" OR address.city =\"Doylefurt\" OR address.city = \"Rutherfordbury\" OR address.city =\"North Vanceville\") "
    			+"AND ( address.country =\"Senegal\" AND (age =78 OR age=30 )))";
    	return N1qlQuery.simple(query);
    }

    private N1qlQuery buildMixedQuery2(String[] terms, int limit , String indexName) {

    	String query = "select country , age "
    			+"from `bucket-1` "
    			+"use index (using FTS) "
    			+"where (address.country=\"Nigeria\" AND (age=31 OR age=33)) "
    			+"OR (ANY num in devices SATISFIES num >= \"060842-712\" AND num<=\"060879-902\" END) "
    			+"AND (routing_number>=1011 AND routing_number<=1020) "
    			+"AND (ANY c IN children SATISFIES c.gender = \"M\"  AND c.age <=8 AND c.first_name = \"Aaron\" END) ";
    	return N1qlQuery.simple(query);
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

