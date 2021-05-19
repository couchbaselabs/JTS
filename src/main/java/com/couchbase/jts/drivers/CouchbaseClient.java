package com.couchbase.jts.drivers;
/*
Created by JNS87
*/
// Imports of general utilities / java library

import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.time.Duration;
import java.lang.System.* ;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

// Imports for Collections
import com.couchbase.client.java.Collection;

// Imports of JTS loggers and other utils

import com.couchbase.jts.properties.TestProperties;
import com.couchbase.jts.logger.GlobalStatusLogger;

// Imports of Cluster settings / managements

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
// Data type for Geo queries
import com.couchbase.client.java.util.Coordinate;

// Imports of the other dependent services
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.java.query.*;
// Search related imports
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchMetrics;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.queries.TermQuery;
import com.couchbase.client.java.search.queries.ConjunctionQuery;
import com.couchbase.client.java.search.queries.DisjunctionQuery;
// Flex Query related imports
import com.couchbase.client.java.query.QueryResult;



public class  CouchbaseClient extends Client{
    private GlobalStatusLogger logWriter = new GlobalStatusLogger();
    // private static volatile CouchbaseEnvironment env = null;
    private static volatile ClusterEnvironment env =  null ;
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
    private boolean enableMutationToken = false;
    private int collection_len = 0 ;
    private volatile Collection collection;

    // An indicator to indicate the number of collections present for the test
    // -1 => no collections
    // 0 Default collection
    // >1 N collections
    private int collectionIndicator= Integer.parseInt(settings.get(TestProperties.TESTSPEC_COLLECTIONS));

    // for docs generated with custom doc gen (created by JNS87)
    // the ids of the docs are of type String.valueOf(long ID) , not its hex equivalent
    private int UseDocIdLong = Integer.parseInt(settings.get(TestProperties.TESTSPEC_DOCID_LONG));

    // Setting the searchQuery Options
    int limit = Integer.parseInt(settings.get(TestProperties.TESTSPEC_QUERY_LIMIT));
    String indexName = settings.get(TestProperties.CBSPEC_INDEX_NAME);

    String scope_prefix = settings.get(TestProperties.TESTSPEC_SCOPE_PREFIX);
    int scope_number = Integer.parseInt(settings.get(TestProperties.TESTSPEC_SCOPE_NUMBER));
    String collection_prefix = settings.get(TestProperties.TESTSPEC_COLLECTIONS_PREFIX);
    int collection_number = Integer.parseInt(settings.get(TestProperties.TESTSPEC_COLLECTIONS_NUMBER));

    // collection_number = number of collections per scope
    private volatile Collection[] collection_list = new Collection[scope_number*collection_number];
    private Cluster cluster;
    private volatile ClusterOptions clusterOptions;
    private Bucket bucket;

    //Search Query variables
    private SearchQuery[] FTSQueries;
    private int totalQueries = 0;
    private SearchQuery queryToRun;

    //Flex Query variables
    private String[] FlexQueries;
    private int FlexTotalQueries = 0;

    private Random rand = new Random();

    public CouchbaseClient(TestProperties workload) throws Exception{
        super(workload);
        connect();
        generateQueries();
    }


    private void connect() throws Exception{

        try {

            synchronized (INIT_COORDINATOR) {
                if(env == null) {
                    // This creates a new ClusterEnvironment with Default Settings
                    env = ClusterEnvironment
                            .builder()
                            .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeout)))
                            .ioConfig(IoConfig.enableMutationTokens(enableMutationToken).numKvConnections(kvEndpoints))
                            .build();
                }
            }
            clusterOptions = ClusterOptions.clusterOptions(getProp(TestProperties.CBSPEC_USER),getProp(TestProperties.CBSPEC_PASSWORD));
            clusterOptions.environment(env);

            cluster = Cluster.connect(getProp(TestProperties.CBSPEC_SERVER),clusterOptions);
            bucket = cluster.bucket(getProp(TestProperties.CBSPEC_CBBUCKET));
            // adding in logic for collection enabling for CC
            logWriter.logMessage("the collectionIndicator is : "+collectionIndicator);
            if(collectionIndicator == -1 || collectionIndicator == 0 ) {
                // In CC the data is present in the default collection for even the bucket level tests
                // This is default collections on the KV side
                collection = bucket.defaultCollection();
            }else {
                // Adding the code for a single non-default scope and non-default collection
                for(int scopeNum = 0 ; scopeNum<scope_number; scopeNum++){
                    for (int collNum = 0; collNum<collection_number; collNum++){
                        collection_list[scopeNum+collNum]=bucket.scope(scope_prefix+String.valueOf(scopeNum+1)).collection(collection_prefix+String.valueOf(collNum+1));
                    }
                }
                collection_len = collection_list.length;

            }

        }catch(Exception ex) {
            throw new Exception("Could not connect to Couchbase Bucket.", ex);
        }
    }

    private void generateQueries() throws Exception {
        String[][] terms = importTerms();
        List <SearchQuery> queryList = null;

        String fieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
        queryList = generateTermQueries(terms,fieldName);
        if((queryList ==null) || (queryList.size()==0)) {
            throw new Exception("Query list is empty! ");
        }
        FTSQueries = queryList.stream().toArray(SearchQuery[]::new);
        totalQueries = FTSQueries.length;

    }

    private List<SearchQuery> generateTermQueries(String[][] terms,  String fieldName)
            throws IllegalArgumentException{
        List<SearchQuery> queryList = new ArrayList<>();
        int size = terms.length;

        for (int i = 0; i<size; i++) {
            int lineSize = terms[i].length;
            if(lineSize > 0) {
                try {
                    SearchQuery query = buildQuery(terms[i], fieldName);
                    queryList.add(query);
                }catch(IndexOutOfBoundsException ex) {
                    continue;
                }
            }
        }
        return queryList ;

    }

    //Query builders
    private SearchQuery buildQuery(String[] terms, String fieldName)
            throws IllegalArgumentException, IndexOutOfBoundsException {

        switch (settings.get(settings.TESTSPEC_QUERY_TYPE)) {
            case TestProperties.CONSTANT_QUERY_TYPE_TERM:
                return buildTermQuery(terms,fieldName);
            case TestProperties.CONSTANT_QUERY_TYPE_AND:
                return buildAndQuery(terms,fieldName);
            case TestProperties.CONSTANT_QUERY_TYPE_OR:
                return buildOrQuery(terms,fieldName);
            case TestProperties.CONSTANT_QUERY_TYPE_AND_OR_OR:
                return buildAndOrOrQuery(terms,fieldName);
            case TestProperties.CONSTANT_QUERY_TYPE_FUZZY:
                return buildFuzzyQuery(terms,fieldName);
            case TestProperties.CONSTANT_QUERY_TYPE_PHRASE:
                return buildPhraseQuery(terms, fieldName);
            case TestProperties.CONSTANT_QUERY_TYPE_PREFIX:
                return buildPrefixQuery(terms, fieldName);
            case TestProperties.CONSTANT_QUERY_TYPE_WILDCARD:
                return buildWildcardQuery(terms, fieldName);

            case TestProperties.CONSTANT_QUERY_TYPE_NUMERIC:
                return buildNumericQuery(terms, fieldName);
            case TestProperties.CONSTANT_QUERY_TYPE_GEO_RADIUS:
                return buildGeoRadiusQuery(terms,fieldName,settings.get(settings.TESTSPEC_GEO_DISTANCE));
            case TestProperties.CONSTANT_QUERY_TYPE_GEO_BOX:
                double latHeight = Double.parseDouble(settings.get(settings.TESTSPEC_GEO_LAT_HEIGHT));
                double lonWidth = Double.parseDouble(settings.get(settings.TESTSPEC_GEO_LON_WIDTH));
                return buildGeoBoundingBoxQuery(terms,fieldName , latHeight,lonWidth);
            case TestProperties.CONSTANT_QUERY_TYPE_GEO_POLYGON:
                return buildGeoPolygonQuery(terms,fieldName);

        }
        throw new IllegalArgumentException("Couchbase query builder: unexpected query type - "
                + settings.get(settings.TESTSPEC_QUERY_TYPE));
    }

    private SearchQuery buildTermQuery(String[] terms, String fieldName) {
        return SearchQuery.term(terms[0]).field(fieldName);
    }
    private SearchQuery buildAndQuery(String[] terms, String fieldName) {
        TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
        TermQuery rt = SearchQuery.term(terms[1]).field(fieldName);
        return SearchQuery.conjuncts(lt,rt);
    }
    private SearchQuery buildOrQuery(String[] terms, String fieldName) {
        TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
        TermQuery rt = SearchQuery.term(terms[1]).field(fieldName);
        return SearchQuery.disjuncts(lt,rt);
    }
    private SearchQuery buildAndOrOrQuery(String[] terms , String fieldName){
        TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
        TermQuery mt = SearchQuery.term(terms[1]).field(fieldName);
        TermQuery rt = SearchQuery.term(terms[2]).field(fieldName);
        DisjunctionQuery disSQ = SearchQuery.disjuncts(mt, rt);
        return SearchQuery.conjuncts(disSQ, lt);
    }
    private SearchQuery buildFuzzyQuery(String[] terms, String fieldName){
        return SearchQuery.term(terms[0]).field(fieldName).fuzziness(Integer.parseInt(terms[1]));
    }
    private SearchQuery buildPhraseQuery(String[] terms, String fieldName){
        return SearchQuery.matchPhrase(terms[0]+ " "+ terms[1]).field(fieldName);
    }
    private SearchQuery buildPrefixQuery(String[] terms, String fieldName){
        return SearchQuery.prefix(terms[0]).field(fieldName);
    }
    private SearchQuery buildWildcardQuery(String [] terms, String fieldName){
        return SearchQuery.wildcard(terms[0]).field(fieldName);
    }

    private SearchQuery buildNumericQuery(String[] terms, String fieldName){
        String[] minmax = terms[0].split(":");
        return  SearchQuery.numericRange().max(Double.parseDouble(minmax[0]), true)
                .min(Double.parseDouble(minmax[1]), true).field(fieldName);

    }
    private SearchQuery buildGeoRadiusQuery(String[] terms,String feildName, String dist){
        //double locationLon, double locationLat, String distance
        double locationLon= Double.parseDouble(terms[0]) ;
        double locationLat = Double.parseDouble(terms[1]);
        String distance = dist;
        return SearchQuery.geoDistance(locationLon, locationLat, distance).field(feildName);
    }

    private SearchQuery buildGeoBoundingBoxQuery(String[] terms, String fieldName, double latHeight, double lonWidth){
        //double topLeftLon, double topLeftLat,double bottomRightLon, double bottomRightLat
        double topLeftLon= Double.parseDouble(terms[0]) ;
        double topLeftLat = Double.parseDouble(terms[1]);
        double bottomRightLon= topLeftLon +lonWidth ;
        double bottomRightLat = topLeftLat - latHeight;
        return  SearchQuery.geoBoundingBox​(topLeftLon,topLeftLat, bottomRightLon,bottomRightLat).field(fieldName);
    }

    private SearchQuery  buildGeoPolygonQuery(String[] terms, String fieldName ){
        List<Coordinate> listOfPts =  new ArrayList<Coordinate>();
        for(int i = 0; i <terms.length;i = i+2)
        {
            double lon = Double.parseDouble(terms[i]);
            double lat = Double.parseDouble(terms[i+1]);
            Coordinate coord = Coordinate.ofLonLat(lon,lat);
            listOfPts.add(coord);
        }

        return SearchQuery.geoPolygon(listOfPts).field(fieldName);

    }



    public float queryAndLatency() {
        long st = System.nanoTime();
        queryToRun = FTSQueries[rand.nextInt(totalQueries)];
        SearchOptions opt = SearchOptions.searchOptions().limit(limit);
        SearchResult res = cluster.searchQuery(indexName,queryToRun,opt);
        long en = System.nanoTime();
        float latency = (float) (en - st) / 1000000;
        int res_size = res.rows().size();
        SearchMetrics metrics = res.metaData().metrics();
        if (res_size > 0 && metrics.maxScore()!= 0 && metrics.totalRows()!= 0){ return latency;}
        return 0;
    }


    public void mutateRandomDoc() {
        long totalDocs = Long.parseLong(settings.get(TestProperties.TESTSPEC_TOTAL_DOCS));
        long docIdLong = Math.abs(rand.nextLong() % totalDocs);
        String docIdHex = "";
        if(UseDocIdLong == 0){
            docIdHex =  Long.toHexString(docIdLong);
        }else{
            docIdHex = String.valueOf(docIdLong);
        }
        String originFieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
        String replaceFieldName = settings.get(TestProperties.TESTSPEC_MUTATION_FIELD);
        // Getting the document content
        if(collection_len == 1){
            collection = collection_list[0];
        }
        if(collection_len > 1){
            collection = collection_list[rand.nextInt(collection_len)];
        }

        GetResult doc = collection.get(docIdHex);
        logWriter.logMessage("this is the doc: "+doc.toString());
        // converting that to a JSON object
        JsonObject mutate_doc = doc.contentAsObject();
        // To get the values we are changing
        Object origin = doc.contentAsObject().getString(originFieldName);
        Object replace = doc.contentAsObject().getString(replaceFieldName);
        // mutating this by interchanging
        mutate_doc.put(originFieldName, replace);
        mutate_doc.put(replaceFieldName, origin);
        // pushing the document
        MutationResult mut_res =  collection.upsert(docIdHex, mutate_doc);


    }

    public String queryDebug() {
        return cluster.searchQuery(indexName,FTSQueries[rand.nextInt(totalQueries)],SearchOptions.searchOptions().limit(limit)).toString();
    }

    public void query() {
        cluster.searchQuery(indexName,FTSQueries[rand.nextInt(totalQueries)],SearchOptions.searchOptions().limit(limit)).toString();
    }

    public Boolean queryAndSuccess() {
        SearchResult res = cluster.searchQuery(indexName,FTSQueries[rand.nextInt(totalQueries)],SearchOptions.searchOptions().limit(limit));
        int res_size = res.rows().size();
        SearchMetrics metrics = res.metaData().metrics();
        if (res_size > 0 && metrics.maxScore()!= 0 && metrics.totalRows()!= 0){ return true;}
        return false;
    }


    private String getProp(String name) {
        return settings.get(name);
    }
    private void fileError(String err) {
        System.out.println(err);
    }


}