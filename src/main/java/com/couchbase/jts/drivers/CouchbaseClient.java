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

//SearchQuery
import com.couchbase.client.java.search.queries.TermQuery;
import com.couchbase.client.java.search.queries.ConjunctionQuery;
import com.couchbase.client.java.search.queries.DisjunctionQuery;
import com.couchbase.client.java.search.queries.DateRangeQuery;
import com.couchbase.client.java.search.queries.TermRangeQuery;
import com.couchbase.client.java.search.queries.PrefixQuery;
import com.couchbase.client.java.search.queries.PhraseQuery;
import com.couchbase.client.java.search.queries.WildcardQuery;
import com.couchbase.client.java.search.queries.GeoBoundingBoxQuery;
import com.couchbase.client.java.search.queries.GeoDistanceQuery;
import com.couchbase.client.java.search.queries.GeoPolygonQuery;

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

	String scopePrefix = settings.get(TestProperties.TESTSPEC_SCOPE_PREFIX);
	int scopeNumber = Integer.parseInt(settings.get(TestProperties.TESTSPEC_SCOPE_NUMBER));
	String collectionPrefix = settings.get(TestProperties.TESTSPEC_COLLECTIONS_PREFIX);
	int collectionNumber = Integer.parseInt(settings.get(TestProperties.TESTSPEC_COLLECTIONS_NUMBER));

// collection_number = number of collections per scope
 String scopeName = scopePrefix + String.valueOf(scopeNumber);
 String collectionName = collectionPrefix + String.valueOf(collectionNumber);
 String[] collectionList= new String[collectionNumber];

	private Cluster cluster;
	private volatile ClusterOptions clusterOptions;
	private Bucket bucket;

	// Collection specific variables
	private Boolean collectionSpecificFlag = Boolean.parseBoolean(settings.get(TestProperties.TESTSPEC_COLLECTIONS_SPECIFIC));
	private Boolean scoreNoneFlag = Boolean.parseBoolean(settings.get(TestProperties.TESTSPEC_SCORE_NONE));

	private SearchOptions opt = SearchOptions.searchOptions().limit(limit).disableScoring(scoreNoneFlag);


	private SearchQuery[] FTSQueries;
	private int totalQueries = 0;
	private SearchQuery queryToRun;

	//Flex Query variables
	private String[] FlexQueries;
	private int FlexTotalQueries = 0;
	private Boolean flexFlag ;
	private String flexQueryToRun;


	private Random rand = new Random();

	public CouchbaseClient(TestProperties workload) throws Exception{
        super(workload);
				connect();
				generateQueries();
    }
		private void generateCollectionSpecificParameters(){
			JsonObject scName = JsonObject.create();
			scName.put("scope", scopeName);
			for(int colNum = 0; colNum<collectionNumber;colNum++){
				collectionList[colNum] = collectionPrefix+String.valueOf(colNum + 1);
			}
			JsonArray colNameList = JsonArray.create();
			colNameList.from(collectionList);
			opt.raw("scope",scName).raw("collections",colNameList);
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
			//logWriter.logMessage("the collectionIndicator is : "+collectionIndicator);
			if(collectionIndicator == -1 || collectionIndicator == 0 ) {
				// In CC the data is present in the default collection for even the bucket level tests
				// This is default collections on the KV side
				collection = bucket.defaultCollection();
			}else {
				// Adding the code for a single non-default scope and non-default collection
				collection = bucket.scope(scopeName).collection(collectionName);

			}

		}catch(Exception ex) {
            throw new Exception("Could not connect to Couchbase Bucket.", ex);
        }
	}

		private void generateQueries() throws Exception {
	String[][] terms = importTerms();
	List <SearchQuery> queryList = null;
	List <String> flexQueryList = null;
	String fieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
	flexFlag = Boolean.parseBoolean(settings.get(TestProperties.TESTSPEC_FLEX));
	if(flexFlag){
		flexQueryList = generateFlexQueries(terms,fieldName);
		if((flexQueryList == null) || (flexQueryList.size()==0)) {
			throw new Exception("Query list is empty! ");
		}
		FlexQueries = flexQueryList.stream().toArray(String[]::new);
		FlexTotalQueries = FlexQueries.length;
	}else{
		queryList = generateTermQueries(terms,fieldName);
		if ((queryList == null) || (queryList.size() == 0)) {
      throw new Exception("Query list is empty!");
    }
		FTSQueries = queryList.stream().toArray(SearchQuery[]::new);
		totalQueries = FTSQueries.length;
	}

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
private List<String> generateFlexQueries(String[][] terms, String fieldName)
	throws IllegalArgumentException{
	List<String> flexQueryList = new ArrayList<>();
	int size = terms.length;
	for (int i = 0; i<size; i++) {
		int lineSize = terms[i].length;
		if(lineSize > 0) {
			try {
				String query = buildFlexQuery();
				flexQueryList.add(query);
			}catch(IndexOutOfBoundsException ex) {
				continue;
			}
		}
	}

	return flexQueryList ;
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
		case TestProperties.CONSTANT_SCORE_NONE:
			return buildScoreNoneQuery(terms, fieldName);
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

private SearchQuery buildScoreNoneQuery(String[] terms, String fieldName){
	TermQuery t1 = SearchQuery.term(terms[0]).field(fieldName);
	TermQuery t2 = SearchQuery.term(terms[1]).field(fieldName);
	TermQuery t3 = SearchQuery.term(terms[2]).field(fieldName);
	TermQuery t4 = SearchQuery.term(terms[3]).field(fieldName);
	TermQuery t5 = SearchQuery.term(terms[4]).field(fieldName);
	ConjunctionQuery conjQ = SearchQuery.conjuncts(t4,t5);
	return SearchQuery.disjuncts(t1,t2,t3,conjQ);

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
private String buildFlexQuery()
	throws IllegalArgumentException, IndexOutOfBoundsException {
	switch(settings.get(settings.TESTSPEC_FLEX_QUERY_TYPE)) {
	    		case TestProperties.CONSTANT_FLEX_QUERY_TYPE_ARRAY :
	    			return buildComplexObjQuery();
	    		case TestProperties.CONSTANT_FLEX_QUERY_TYPE_MIXED1:
	    			return buildMixedQuery1();
	    		case TestProperties.CONSTANT_FLEX_QUERY_TYPE_MIXED2:
	    			return buildMixedQuery2();
	    		}
	    		throw new IllegalArgumentException("Couchbase query builder: unexpected flex query type.");
}

private String buildComplexObjQuery() {
		 String query ="SELECT devices, company_name, first_name "
						 + "FROM `bucket-1` USE INDEX( perf_fts_index USING FTS) "
				 + "WHERE (((ANY c IN children SATISFIES c.gender = \"M\"  AND c.age <=8 AND c.first_name = \"Aaron\" END) "
				 + "OR (ANY num in devices SATISFIES num >= \"070842-712\" AND num<=\"070875-000\" END) ) ) "
				 + "AND ((ANY num in devices SATISFIES num >= \"060842-712\" AND num<=\"060843-712\" END) "
				 + "OR  (ANY c in children SATISFIES (c.first_name =\"Tyra\" or c.first_name =\"Aaron\") "
				 + "AND c.gender = \"F\" AND c.age>=10 AND c.age<=13 END))OR(ANY c IN children SATISFIES c.gender = \"F\" "
				 + "AND c.age <=5 AND (first_name=\"Sienna\" OR first_name= \"Pattie\" ) END )";
		 return query;
	 }

private String buildMixedQuery1() {
		 String query = "select first_name , routing_number, city , country, age "
				 + "from `bucket-1` USE index (using FTS) "
				 +"where ((routing_number>=1011 AND routing_number<=1020) "
				 +"OR (address.city =\"Schoenview\" OR address.city =\"Doylefurt\" OR address.city = \"Rutherfordbury\" OR address.city =\"North Vanceville\") "
				 +"AND ( address.country =\"Senegal\" AND (age =78 OR age=30 )))";
		 return query;
	}

private String buildMixedQuery2() {
		 String query = "select country , age "
				 +"from `bucket-1` "
				 +"use index (using FTS) "
				 +"where (address.country=\"Nigeria\" AND (age=31 OR age=33)) "
				 +"OR (ANY num in devices SATISFIES num >= \"060842-712\" AND num<=\"060879-902\" END) "
				 +"AND (routing_number>=1011 AND routing_number<=1020) "
				 +"AND (ANY c IN children SATISFIES c.gender = \"M\"  AND c.age <=8 AND c.first_name = \"Aaron\" END) ";
		 return query;
	}


public float queryAndLatency() {
	long st = 0;
	long en = 0;
	queryToRun = FTSQueries[rand.nextInt(totalQueries)];
	flexQueryToRun = "";
	if(flexFlag){
		flexQueryToRun = FlexQueries[rand.nextInt(FlexTotalQueries)];
	}

	if (collectionSpecificFlag){
		generateCollectionSpecificParameters();
	}
	SearchResult res = null;
	QueryResult flexRes = null ;
	if(flexFlag){
		st = System.nanoTime();
		flexRes = cluster.query(flexQueryToRun);
		//logWriter.logMessage("the resultSet is :"+ flexRes.toString());
		en = System.nanoTime();
		//logWriter.logMessage(res.toString());
	}else{
		st = System.nanoTime();
		res = cluster.searchQuery(indexName,queryToRun,opt);
		en = System.nanoTime();
	}
	logWriter.logMessage("this is the result: "+res.toString());
	float latency = (float) (en - st) / 1000000;
	if (flexFlag){
		if (String.valueOf(flexRes.metaData().status()) == "SUCCESS"){return latency; }
        	fileError(flexRes.toString());
	}else{
		int res_size = res.rows().size();
		SearchMetrics metrics = res.metaData().metrics();
		if (res_size > 0 && metrics.totalRows()!= 0){ return latency;}
	}

	return 0;
}


public void mutateRandomDoc() {
	long totalDocs = Long.parseLong(settings.get(TestProperties.TESTSPEC_TOTAL_DOCS));
	long docIdLong = Math.abs(rand.nextLong() % totalDocs);
	String docIdHex = "";
	//logWriter.logMessage("this is the doc: "+doc.toString());
	if(UseDocIdLong == 0){
		docIdHex =  Long.toHexString(docIdLong);
	}else{
		docIdHex = String.valueOf(docIdLong);
	}
	String originFieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
	String replaceFieldName = settings.get(TestProperties.TESTSPEC_MUTATION_FIELD);

	GetResult doc = collection.get(docIdHex);
	//logWriter.logMessage("this is the doc: "+doc.toString());
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
	if (collectionSpecificFlag){
		generateCollectionSpecificParameters();
	}
	if(flexFlag){
		return cluster.query(FlexQueries[rand.nextInt(FlexTotalQueries)]).toString();
	}else{
		return cluster.searchQuery(indexName,FTSQueries[rand.nextInt(totalQueries)],opt).toString();
	}
}

public void query() {
	if (collectionSpecificFlag){
		generateCollectionSpecificParameters();
	}
	if(flexFlag){
		 cluster.query(FlexQueries[rand.nextInt(FlexTotalQueries)]).toString();
	}else{
		cluster.searchQuery(indexName,FTSQueries[rand.nextInt(totalQueries)],opt).toString();
	}

}

public Boolean queryAndSuccess() {
	if (collectionSpecificFlag){
		generateCollectionSpecificParameters();
	}
	if(flexFlag){
		QueryResult flexRes = cluster.query(FlexQueries[rand.nextInt(FlexTotalQueries)]);
		//logWriter.logMessage(flexRes.toString());
		long resultCount = 2;
		//logWriter.logMessage("This is resultCount: "+flexRes.metaData().status());
		if (String.valueOf(flexRes.metaData().status()) == "SUCCESS"){return true; }
		return false;
	}else{
		SearchResult res = cluster.searchQuery(indexName,FTSQueries[rand.nextInt(totalQueries)],opt);
		int res_size = res.rows().size();
		SearchMetrics metrics = res.metaData().metrics();
		if (res_size > 0 && metrics.totalRows()!= 0){ return true;}
		return false;
	}
}


 private String getProp(String name) {
				return settings.get(name);
		}
 private void fileError(String err) {
				System.out.println(err);
		}


}
