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
			if(collectionIndicator == -1 || collectionIndicator == 0 ) {
				// In CC the data is present in the default collection for even the bucket level tests
				// This is default collections on the KV side
				collection = bucket.defaultCollection();
			}else {
				// Adding the code for a single non-default scope and non-default collection
				// need to create a function to randomize the mutations over the collections
				collection = bucket.scope("scope-1").collection("collection-1");
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
	GetResult doc = collection.get(docIdHex);
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
