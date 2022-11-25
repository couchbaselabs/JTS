package com.couchbase.jts.drivers;

import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.time.Duration;
import java.lang.System.*;
import java.sql.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.HashSet;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.couchbase.jts.properties.TestProperties;
import com.couchbase.jts.logger.GlobalStatusLogger;

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.util.Coordinate;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchMetrics;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.queries.TermQuery;
import com.couchbase.client.java.search.queries.ConjunctionQuery;
import com.couchbase.client.java.search.queries.DateRangeQuery;
import com.couchbase.client.java.search.queries.DisjunctionQuery;
import com.couchbase.client.java.query.QueryResult;

public class CouchbaseClient extends Client {
	private GlobalStatusLogger logWriter = new GlobalStatusLogger();
	private static volatile ClusterEnvironment env = null;
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

	private boolean collectionsEnabled = Boolean
			.parseBoolean(settings.get(TestProperties.TESTSPEC_COLLECTIONS_ENABLED));
	private boolean index_map_provided;
	private String collection_query_mode = settings.get(TestProperties.TESTSPEC_COLLECTION_QUERY_MODE);
	private int collection_specific_count = Integer
			.parseInt(settings.get(TestProperties.TESTSPEC_COLLECTION_SPECIFIC_COUNT));
	private String fts_index_map_raw = settings.get(TestProperties.TESTSPEC_FTS_INDEX_MAP);
	private JSONObject fts_index_json;
	private List<String> fts_index_list;
	private List<String> collections_list;
	private int numCollections;

	int limit = Integer.parseInt(settings.get(TestProperties.TESTSPEC_QUERY_LIMIT));
	private String indexName = settings.get(TestProperties.CBSPEC_INDEX_NAME);
	private Cluster cluster;
	private volatile ClusterOptions clusterOptions;
	private Bucket bucket;
	private SearchQuery[] FTSQueries;
	private int totalQueries = 0;
	private SearchQuery queryToRun;
	private String[] FlexQueries;
	private int FlexTotalQueries = 0;
	private Random rand = new Random();

	public CouchbaseClient(TestProperties workload) throws Exception {
		super(workload);
		setup();
		connect();
		generateQueries();
	}

	private void setup() throws Exception {
		if (!fts_index_map_raw.equals("")) {
			index_map_provided = true;
			JSONParser jsonParser = new JSONParser();
			Object obj = jsonParser.parse(fts_index_map_raw);
			fts_index_json = (JSONObject) obj;
			fts_index_list = new ArrayList<String>(fts_index_json.keySet());
			Set target_set = new HashSet();

			for (String index : fts_index_list) {
				JSONObject index_targets = (JSONObject) fts_index_json.get(index);
				String targetScope = (String) index_targets.get("scope");
				ArrayList<String> targetCollections = (ArrayList<String>) index_targets.get("collections");

				for (String targetCollection : targetCollections) {
					target_set.add(targetScope + "." + targetCollection);
				}
			}

			collections_list = new ArrayList<String>(target_set);
			numCollections = collections_list.size();

		} else {
			index_map_provided = false;
			Set target_set = new HashSet();
			target_set.add("_default._default");
			collections_list = new ArrayList<String>(target_set);
			numCollections = collections_list.size();
		}

		if (collectionsEnabled && !index_map_provided) {
			throw new Exception("index map required when collections enabled");
		}
	}

	private void connect() throws Exception {
		try {
			String connection_prefix = "";
			synchronized (INIT_COORDINATOR) {
				if (env == null) {
					ClusterEnvironment.Builder builder = ClusterEnvironment
							.builder()
							.timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeout)))
							.ioConfig(IoConfig.enableMutationTokens(enableMutationToken).numKvConnections(kvEndpoints));

					if (getProp(TestProperties.CBSPEC_SSLMODE).equals("capella")) {
						builder.securityConfig(SecurityConfig
								.enableTls(true)
								.trustManagerFactory(InsecureTrustManagerFactory.INSTANCE));
						connection_prefix = "couchbases://";
					}
					env = builder.build();
				}
			}
			clusterOptions = ClusterOptions.clusterOptions(getProp(TestProperties.CBSPEC_USER),
					getProp(TestProperties.CBSPEC_PASSWORD));

			clusterOptions.environment(env);
			cluster = Cluster.connect(connection_prefix + getProp(TestProperties.CBSPEC_SERVER), clusterOptions);
			bucket = cluster.bucket(getProp(TestProperties.CBSPEC_CBBUCKET));
		} catch (Exception ex) {
			throw new Exception("Could not connect to Couchbase Bucket.", ex);
		}
	}

	private void generateQueries() throws Exception {
		String[][] terms = importTerms();
		List<SearchQuery> queryList = null;
		String fieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
		queryList = generateTermQueries(terms, fieldName);
		if ((queryList == null) || (queryList.size() == 0)) {
			throw new Exception("Query list is empty! ");
		}
		FTSQueries = queryList.stream().toArray(SearchQuery[]::new);
		totalQueries = FTSQueries.length;
	}

	private List<SearchQuery> generateTermQueries(String[][] terms, String fieldName) throws IllegalArgumentException {
		List<SearchQuery> queryList = new ArrayList<>();
		int size = terms.length;
		for (int i = 0; i < size; i++) {
			int lineSize = terms[i].length;
			if (lineSize > 0) {
				try {
					SearchQuery query = buildQuery(terms[i], fieldName);
					queryList.add(query);
				} catch (IndexOutOfBoundsException ex) {
					continue;
				}
			}
		}
		return queryList;
	}

	private SearchQuery buildQuery(String[] terms, String fieldName)
			throws IllegalArgumentException, IndexOutOfBoundsException {
		switch (settings.get(settings.TESTSPEC_QUERY_TYPE)) {
			case TestProperties.CONSTANT_QUERY_TYPE_TERM:
				return buildTermQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_AND:
				return buildAndQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_OR:
				return buildOrQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_AND_OR_OR:
				return buildAndOrOrQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_FUZZY:
				return buildFuzzyQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_PHRASE:
				return buildPhraseQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_PREFIX:
				return buildPrefixQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_WILDCARD:
				return buildWildcardQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_NUMERIC:
				return buildNumericQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_DATERANGE:
				return buildDateQuery(terms, indexName, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_GEO_RADIUS:
				return buildGeoRadiusQuery(terms, fieldName, settings.get(settings.TESTSPEC_GEO_DISTANCE));
			case TestProperties.CONSTANT_QUERY_TYPE_GEO_BOX:
				double latHeight = Double.parseDouble(settings.get(settings.TESTSPEC_GEO_LAT_HEIGHT));
				double lonWidth = Double.parseDouble(settings.get(settings.TESTSPEC_GEO_LON_WIDTH));
				return buildGeoBoundingBoxQuery(terms, fieldName, latHeight, lonWidth);
			case TestProperties.CONSTANT_QUERY_TYPE_GEO_POLYGON:
				return buildGeoPolygonQuery(terms, fieldName);
		}
		throw new IllegalArgumentException(
				"Couchbase query builder: unexpected query type - " +
						settings.get(settings.TESTSPEC_QUERY_TYPE));
	}

	private SearchQuery buildTermQuery(String[] terms, String fieldName) {
		return SearchQuery.term(terms[0]).field(fieldName);
	}

	private SearchQuery buildAndQuery(String[] terms, String fieldName) {
		TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
		TermQuery rt = SearchQuery.term(terms[1]).field(fieldName);
		return SearchQuery.conjuncts(lt, rt);
	}

	private SearchQuery buildOrQuery(String[] terms, String fieldName) {
		TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
		TermQuery rt = SearchQuery.term(terms[1]).field(fieldName);
		return SearchQuery.disjuncts(lt, rt);
	}

	private SearchQuery buildAndOrOrQuery(String[] terms, String fieldName) {
		TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
		TermQuery mt = SearchQuery.term(terms[1]).field(fieldName);
		TermQuery rt = SearchQuery.term(terms[2]).field(fieldName);
		DisjunctionQuery disSQ = SearchQuery.disjuncts(mt, rt);
		return SearchQuery.conjuncts(disSQ, lt);
	}

	private SearchQuery buildFuzzyQuery(String[] terms, String fieldName) {
		return SearchQuery.term(terms[0]).field(fieldName).fuzziness(Integer.parseInt(terms[1]));
	}

	private SearchQuery buildPhraseQuery(String[] terms, String fieldName) {
		return SearchQuery.matchPhrase(terms[0] + " " + terms[1]).field(fieldName);
	}

	private SearchQuery buildPrefixQuery(String[] terms, String fieldName) {
		return SearchQuery.prefix(terms[0]).field(fieldName);
	}

	private SearchQuery buildWildcardQuery(String[] terms, String fieldName) {
		return SearchQuery.wildcard(terms[0]).field(fieldName);
	}

	private SearchQuery buildNumericQuery(String[] terms, String fieldName) {
		String[] minmax = terms[0].split(":");
		return SearchQuery.numericRange().max(Double.parseDouble(minmax[0]), true)
				.min(Double.parseDouble(minmax[1]), true).field(fieldName);
	}

	private SearchQuery buildDateQuery(String[] terms, String indexName, String fieldName) {
		String[] startend = terms[0].split(":");
		return SearchQuery.dateRange()
				.start(startend[0], true)
				.end(startend[1], true).field(fieldName);

	}

	private SearchQuery buildGeoRadiusQuery(String[] terms, String feildName, String dist) {
		double locationLon = Double.parseDouble(terms[0]);
		double locationLat = Double.parseDouble(terms[1]);
		String distance = dist;
		return SearchQuery.geoDistance(locationLon, locationLat, distance).field(feildName);
	}

	private SearchQuery buildGeoBoundingBoxQuery(String[] terms, String fieldName, double latHeight, double lonWidth) {
		double topLeftLon = Double.parseDouble(terms[0]);
		double topLeftLat = Double.parseDouble(terms[1]);
		double bottomRightLon = topLeftLon + lonWidth;
		double bottomRightLat = topLeftLat - latHeight;
		return SearchQuery.geoBoundingBox​(topLeftLon, topLeftLat, bottomRightLon, bottomRightLat).field(fieldName);
	}

	private SearchQuery buildGeoPolygonQuery(String[] terms, String fieldName) {
		List<Coordinate> listOfPts = new ArrayList<Coordinate>();
		for (int i = 0; i < terms.length; i = i + 2) {
			double lon = Double.parseDouble(terms[i]);
			double lat = Double.parseDouble(terms[i + 1]);
			Coordinate coord = Coordinate.ofLonLat(lon, lat);
			listOfPts.add(coord);
		}
		return SearchQuery.geoPolygon(listOfPts).field(fieldName);
	}

	public void mutateRandomDoc() {
		long totalDocs = Long.parseLong(settings.get(TestProperties.TESTSPEC_TOTAL_DOCS));
		long docPerCollection = totalDocs / numCollections;
		long docIdLong = Math.abs(rand.nextLong() % docPerCollection);

		String docIdHex = Long.toHexString(docIdLong);
		String originFieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
		String replaceFieldName = settings.get(TestProperties.TESTSPEC_MUTATION_FIELD);

		Collection collection;
		String target = getRandomCollection();
		if (target.equals("_default._default")) {
			collection = bucket.defaultCollection();
		} else {
			String[] scope_collection = target.split("\\.");
			String targetScope = scope_collection[0];
			String targetColl = scope_collection[1];
			collection = bucket.scope(targetScope).collection(targetColl);
		}
		GetResult doc = collection.get(docIdHex);
		JsonObject mutate_doc = doc.contentAsObject();
		Object origin = doc.contentAsObject().getString(originFieldName);
		Object replace = doc.contentAsObject().getString(replaceFieldName);
		mutate_doc.put(originFieldName, replace);
		mutate_doc.put(replaceFieldName, origin);
		MutationResult mut_res = collection.upsert(docIdHex, mutate_doc);
	}

	public String queryDebug() {
		String indexToQuery = indexName;
		if (index_map_provided) {
			indexToQuery = getRandomIndex();
		}
		SearchOptions opt = genSearchOpts(indexToQuery);
		return cluster.searchQuery(indexToQuery, FTSQueries[rand.nextInt(totalQueries)], opt).toString();
	}

	public void query() {
		String indexToQuery = indexName;
		if (index_map_provided) {
			indexToQuery = getRandomIndex();
		}
		SearchOptions opt = genSearchOpts(indexToQuery);
		cluster.searchQuery(indexToQuery, FTSQueries[rand.nextInt(totalQueries)], opt).toString();
	}

	public float queryAndLatency() {
		String indexToQuery = indexName;
		if (index_map_provided) {
			indexToQuery = getRandomIndex();
		}
		SearchOptions opt = genSearchOpts(indexToQuery);
		queryToRun = FTSQueries[rand.nextInt(totalQueries)];
		long st = System.nanoTime();
		SearchResult res = cluster.searchQuery(indexToQuery, queryToRun, opt);
		System.out.println(queryToRun);
		System.out.println(res);
		long en = System.nanoTime();
		float latency = (float) (en - st) / 1000000;
		int res_size = res.rows().size();
		SearchMetrics metrics = res.metaData().metrics();
		if (res_size > 0 && metrics.maxScore() != 0 && metrics.totalRows() != 0) {
			return latency;
		}
		return 0;
	}

	public Boolean queryAndSuccess() {
		String indexToQuery = indexName;
		if (index_map_provided) {
			indexToQuery = getRandomIndex();
		}
		SearchOptions opt = genSearchOpts(indexToQuery);
		SearchResult res = cluster.searchQuery(indexToQuery, FTSQueries[rand.nextInt(totalQueries)], opt);
		int res_size = res.rows().size();
		SearchMetrics metrics = res.metaData().metrics();
		if (res_size > 0 && metrics.maxScore() != 0 && metrics.totalRows() != 0) {
			return true;
		}
		return false;
	}

	private String getRandomIndex() {
		return fts_index_list.get(rand.nextInt(fts_index_list.size()));
	}

	private String getRandomCollection() {
		return collections_list.get(rand.nextInt(collections_list.size()));
	}

	private ArrayList<String> getRandomCollections(ArrayList<String> collectionList) {
		ArrayList<String> subsetList = new ArrayList<>();
		Set coll_set = new HashSet();
		int num_coll = collectionList.size();
		while (coll_set.size() < collection_specific_count) {
			coll_set.add(collectionList.get(rand.nextInt(num_coll)));
		}
		subsetList.addAll(coll_set);
		return subsetList;
	}

	private SearchOptions genSearchOpts(String indexToQuery) {
		SearchOptions opt = SearchOptions.searchOptions().limit(limit);
		if (collectionsEnabled && collection_query_mode.equals("collection_specific")) {
			JSONObject index_targets = (JSONObject) fts_index_json.get(indexToQuery);
			JsonObject scopeJson = JsonObject.create();
			String targetScope = (String) index_targets.get("scope");
			scopeJson.put("scope", targetScope);

			ArrayList<String> targetCollections = (ArrayList<String>) index_targets.get("collections");
			ArrayList<String> randomCollections = getRandomCollections(targetCollections);
			JsonArray colJson = JsonArray.from(randomCollections);
			opt.raw("scope", scopeJson).raw("collections", colJson);
		}
		return opt;
	}

	private String getProp(String name) {
		return settings.get(name);
	}

	private void fileError(String err) {
		System.out.println(err);
	}
}
