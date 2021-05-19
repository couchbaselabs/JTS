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
import java.util.Set;
import java.util.HashSet;

// imports for the JSON parser
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

// Imports for Collections
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.Collection;

// Imports of JTS loggers and other utils

import com.couchbase.jts.properties.TestProperties;
import com.couchbase.jts.logger.GlobalStatusLogger;

// Imports of Cluster settings / managements

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
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

	private boolean collectionsEnabled = Boolean.parseBoolean(settings.get(TestProperties.TESTSPEC_COLLECTIONS_ENABLED));
	private boolean indexMapProvided;
	// This will mark if the query type is collection specific or blanket
	private String collectionsQueryMode = settings.get(TestProperties.TESTSPEC_COLLECTION_QUERY_MODE);
	// to determine the number of collections in the collection specific list
	private int collectionSpecificCount =  Integer.parseInt(settings.get(TestProperties.TESTSPEC_COLLECTION_SPECIFIC_COUNT));
	private String ftsIndexMapRaw = settings.get(TestProperties.TESTSPEC_FTS_INDEX_MAP);
	private JSONObject ftsIndexJson;
	private List<String> ftsIndexList;
	private List<String> collectionsList;
	private String scopeName;
	private ArrayList<String> collectionsListRaw ;
	private int numCollections;


	// Setting the searchQuery Options
	int limit = Integer.parseInt(settings.get(TestProperties.TESTSPEC_QUERY_LIMIT));
	String indexName = settings.get(TestProperties.CBSPEC_INDEX_NAME);
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
		setup();
		connect();
		generateQueries();
	}

	private void setup() throws Exception{
		if(!ftsIndexMapRaw.equals("")){
			indexMapProvided= true;
			JSONParser jsonParser = new JSONParser();
			Object obj = jsonParser.parse(ftsIndexMapRaw);
			ftsIndexJson = (JSONObject) obj;
			Set targetSet = new HashSet();

			// it is a single index
			JSONObject indexDetails = (JSONObject) ftsIndexJson.get(indexName);
			// to get the scope name
			scopeName = (String) indexDetails.get("scope");
			// To get the list of Collections
			collectionsListRaw = (ArrayList<String>) indexDetails.get("collections");
			for(String collectionName : collectionsListRaw){
				targetSet.add(scopeName+"."+collectionName);
			}
			collectionsList = new ArrayList<String>(targetSet);
			numCollections = collectionsList.size();
		}else{
			indexMapProvided = false;
			Set targetSet = new HashSet();
			targetSet.add("_default._default");
			collectionsList = new ArrayList<String>(targetSet);
			numCollections = collectionsList.size();
		}

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

	private List<SearchQuery> generateTermQueries(String[][] terms,  String fieldName) throws IllegalArgumentException{
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
		return  SearchQuery.geoBoundingBox(topLeftLon,topLeftLat, bottomRightLon,bottomRightLat).field(fieldName);
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

	private ArrayList<String> getCollectionList(ArrayList<String> collectionList){
		ArrayList<String> subsetList = new ArrayList<>();
		// If all the collections are included
		if (collectionSpecificCount == numCollections){
			subsetList = collectionList;
		}
		if(collectionSpecificCount > 1 && collectionSpecificCount < numCollections){
			int startIndex = rand.nextInt(numCollections);
			int endIndex = startIndex + collectionSpecificCount;

			if (endIndex < numCollections){
				subsetList = new ArrayList<String>(collectionList.subList(startIndex,endIndex));
			}
			if(endIndex >= numCollections){
				subsetList = new ArrayList<String>(collectionList.subList(startIndex,numCollections));
				endIndex = endIndex - numCollections;
				ArrayList <String> tempList = new ArrayList<String> (collectionsList.subList(0,endIndex));
				for (String collectionName: tempList){
					subsetList.add(collectionName);
				}
			}
		}
		if (collectionSpecificCount == 1){
			subsetList.add(collectionList.get(rand.nextInt(numCollections)));
		}
		return subsetList;
	}

	public SearchOptions genSearchOpts(String indexToQuery){
		SearchOptions opt = SearchOptions.searchOptions().limit(limit);
		if(collectionsEnabled && collectionsQueryMode.equals("collection_specific")){
			// Setting the scope name for the collection Specific
			JsonObject scopeJson = JsonObject.create();
			scopeJson.put("scope",scopeName);

			// Setting the collection name for the collection specific queries
			ArrayList<String> targetCollections = getCollectionList(collectionsListRaw);
			JsonArray colJson = JsonArray.from(targetCollections);
			opt.raw("scope",scopeJson).raw("collections",colJson);
		}
		return opt;
	}


	public float queryAndLatency() {
		queryToRun = FTSQueries[rand.nextInt(totalQueries)];
		// Setting the limit and the other options of the SearchQuery
		SearchOptions opt = genSearchOpts(indexName);
		long st = System.nanoTime();
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
		long docsPerCollection = totalDocs/numCollections;
		long docIdLong = Math.abs(rand.nextLong() % docsPerCollection);
		String docIdHex =  Long.toHexString(docIdLong);
		String originFieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
		String replaceFieldName = settings.get(TestProperties.TESTSPEC_MUTATION_FIELD);

		Collection collection ;
		// to choose the random collection
		String targetCollection= collectionsList.get(rand.nextInt(numCollections));
		if(targetCollection.equals("_default._default")){
			collection = bucket.defaultCollection();
		}else{
			collection = bucket.scope(scopeName).collection(targetCollection);
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
		return cluster.searchQuery(indexName,FTSQueries[rand.nextInt(totalQueries)],genSearchOpts(indexName)).toString();
	}

	public void query() {
		cluster.searchQuery(indexName,FTSQueries[rand.nextInt(totalQueries)],genSearchOpts(indexName)).toString();
	}

	public Boolean queryAndSuccess() {
		SearchResult res = cluster.searchQuery(indexName,FTSQueries[rand.nextInt(totalQueries)],genSearchOpts(indexName));
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