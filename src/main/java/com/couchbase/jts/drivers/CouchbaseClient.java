package com.couchbase.jts.drivers;

import java.util.Random;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.time.Duration;
import java.lang.System.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.HashSet;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
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
import com.couchbase.client.java.search.queries.NumericRangeQuery;
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
	private int Search_Query_timeout = Integer.parseInt(settings.get(TestProperties.SEARCH_QUERY_TIMEOUT_IN_SEC));
	private String RawJsonStrig = settings.get(TestProperties.TESTSPEC_FTS_RAW_QUERY_MAP);
	private int k_nearest_neighbour = Integer.parseInt(settings.get(TestProperties.K_NEAREST_NEIGHBOUR));
	private String secondfieldName = settings.get(TestProperties.TESTSPEC_QUERY_FIELD2);
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
			case TestProperties.CONSTANT_QUERY_TYPE_MATCH:
				return buildMatchQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_GEOSHAPE:
				return buildGeoJsonQuery(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_VECTOR:
				return buildVectorSearchQuery(terms, fieldName,'A');
			case TestProperties.CONSTANT_QUERY_TYPE_MULTIPLE_VECTOR:
				// This case is being handled in the injectParams class itself
				// but it can be implemented using a new function if we pass specific
				// new query for second knn .. Right now I am using same KNN in both fields.
				return buildVectorSearchQuery(terms, fieldName,'A');
			case TestProperties.CONSTANT_QUERY_TYPE_NUMERIC_VECTOR:
				return buildVectorSearchQuery(terms, fieldName, 'B');
			case TestProperties.CONSTANT_QUERY_TYPE_TEXT_VECTOR:
				return buildVectorSearchQuery(terms, fieldName, 'C');
			case TestProperties.CONSTANT_QUERY_TYPE_BASE64_VECTOR:
				return buildVectorBase64SearchQuery(terms, fieldName, 'A');
			case TestProperties.CONSTANT_QUERY_TYPE_AND_TERM_NUMBER:
			 	return buildAndQueryForNumber(terms, fieldName);
			case TestProperties.CONSTANT_QUERY_TYPE_AND_NUMBER_NUMBER:
				return buildAndQueryForNumberAndNumber(terms, fieldName);
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

	private SearchQuery buildMatchQuery(String[] terms, String fieldName) {
		return SearchQuery.match(terms[0]).field(fieldName);
	}

	public class RawGeoJsonQuery extends SearchQuery {

		private List<Coordinate> coordinates;
		private String field;
		private String relation;
		private String shape;

		public RawGeoJsonQuery(List<Coordinate> coordinates, String shape, String relation) {
			super();
			this.coordinates = coordinates;
			this.relation = relation;
			this.shape = shape;
		}

		/**
		 * Allows to specify which field the query should apply to (default is null).
		 *
		 * @param field the name of the field in the index/document (if null it is not
		 *              considered).
		 * @return this {@link RawGeoJsonQuery} for chaining purposes.
		 */
		public RawGeoJsonQuery field(final String field) {
			this.field = field;
			return this;
		}

		@Override
		public RawGeoJsonQuery boost(final double boost) {
			super.boost(boost);
			return this;
		}

		private JsonObject PointGeometry() {
			final JsonArray points = JsonArray.from(coordinates.get(0).lon(), coordinates.get(0).lat());

			final JsonObject geometry = JsonObject.create().put("shape", JsonObject.create()
					.put("coordinates", points)
					.put("type", shape))
					.put("relation", relation);
			return geometry;
		}

		private JsonObject CircleGeometry() {
			final JsonArray points = JsonArray.from(coordinates.get(0).lon(), coordinates.get(0).lat());

			final JsonObject geometry = JsonObject.create().put("shape", JsonObject.create()
					.put("coordinates", points)
					.put("type", shape)
					.put("radius", settings.get(settings.TESTSPEC_GEO_DISTANCE)))
					.put("relation", relation);
			return geometry;
		}

		private JsonObject PolygonGeometry() {
			JsonArray points = JsonArray.create();
			for (int i=0; i < coordinates.size();i++){
				points.add(JsonArray.from(coordinates.get(i).lon(), coordinates.get(i).lat()));
			}

			final JsonObject geometry = JsonObject.create().put("shape", JsonObject.create()
					.put("coordinates", JsonArray.from(points))
					.put("type", shape))
					.put("relation", relation);
			return geometry;
		}

		private JsonObject LinestringGeometry() {

			JsonArray points = JsonArray.create();
			for (int i=0; i < coordinates.size();i++){
				points.add(JsonArray.from(coordinates.get(i).lon(), coordinates.get(i).lat()));
			}

			final JsonObject geometry = JsonObject.create().put("shape", JsonObject.create()
					.put("coordinates", points)
					.put("type", shape))
					.put("relation", relation);
			return geometry;
		}

		@Override
		protected void injectParams(final JsonObject input) {
			JsonObject geometry = JsonObject.create();
			switch (shape) {
				case "circle":
					geometry = CircleGeometry();
					break;
				case "polygon":
					geometry = PolygonGeometry();
					break;
				case "point":
					geometry = PointGeometry();
					break;
				case "linestring":
					geometry = LinestringGeometry();
					break;
				default:
					throw new IllegalArgumentException(
							"Couchbase query builder: unsupportedGeoJson Query Shape" +
									shape);
			}

			if (field != null) {
				input.put("field", field);
			}
			input.put("geometry", geometry);

		}
	}

	public class VectorSearchQuery extends SearchQuery {

		private String field;
		private JsonArray vectArray ;
		private int k;
		private JsonObject queryObject;
		private String vectorBase64String;

		public VectorSearchQuery(JsonArray vectors, int k) {
			super();
			this.vectArray = vectors;
			this.k = k;
		}

		public VectorSearchQuery(String vectorBase64String, int k) {
			super();
			this.vectorBase64String = vectorBase64String;
			this.k = k;
		}
		/**
		 * Allows to specify which field the query should apply to (default is null).
		 *
		 * @param field the name of the field in the index/document (if null it is not
		 *              considered).
		 * @return this {@link VectorSearchQuery} for chaining purposes.
		 */
		public VectorSearchQuery field(final String field) {
			this.field = field;
			return this;
		}

		@Override
		public JsonObject export() {
			JsonObject result = JsonObject.create();
			injectParams(result);
	
			JsonObject queryJson = JsonObject.create();
			injectParamsAndBoost(queryJson);
			return queryJson;
		}

		@Override
		public VectorSearchQuery boost(final double boost) {
			super.boost(boost);
			return this;
		}

		public VectorSearchQuery queryObject(final JsonObject queryObject){
			this.queryObject = queryObject;
			return this;
		}

		@Override
		protected void injectParams(final JsonObject input) {
			JsonArray knn = JsonArray.create();
			JsonObject knnObject = JsonObject.create().put("field", field).put("k", k);
			// knn.add(JsonObject.create().put("field", field).put("vector", vectArray).put("k", k));
			if (TestProperties.CONSTANT_QUERY_TYPE_BASE64_VECTOR.equals(settings.get(settings.TESTSPEC_QUERY_TYPE))){
				knnObject.put("vector_base64", vectorBase64String);
			} else {
				knnObject.put("vector", vectArray);
			}
			knn.add(knnObject);
			if (TestProperties.CONSTANT_QUERY_TYPE_MULTIPLE_VECTOR.equals(settings.get(settings.TESTSPEC_QUERY_TYPE))) {
				knn.add(JsonObject.create().put("field", secondfieldName).put("vector", vectArray).put("k", k));
			}
			input.put("knn", knn);
			input.put("query", queryObject);
			input.put("sort", JsonArray.create().add("-_score") );
		}
	}

	private RawGeoJsonQuery buildGeoJsonQuery(String[] terms, String fieldName) {

		List<Coordinate> listOfPts = new ArrayList<Coordinate>();
		for (int i = 0; i < terms.length; i = i + 2) {
			double lon = Double.parseDouble(terms[i]);
			double lat = Double.parseDouble(terms[i + 1]);
			Coordinate coord = Coordinate.ofLonLat(lon, lat);
			listOfPts.add(coord);
		}
		String shape = settings.get(settings.TESTSPEC_GEOJSON_QUERY_TYPE);
		String relation = settings.get(settings.TESTSPEC_GEOJSON_QUERY_RELATION);
		RawGeoJsonQuery rawjson = new RawGeoJsonQuery(listOfPts, shape, relation).field(fieldName);
		return rawjson;
		// "query": {
		// "field": "<<fieldName>>",
		// "geometry": {
		// "shape": {
		// "type": "point",
		// "coordinates": [1.954764, 50.962097]
		// },
		// "relation": "intersects"
		// }
		// }
		// }
	}

	private JsonObject buildQueryObjectForVectorSearch(String[] terms, String fieldName, char caseType){
		// Cases definition
		// A --> Pure KNN or multiKNN
		// B --> Numeric
		// C --> Text 
		JsonObject queryObject;
		switch (caseType) {
			case 'A':
				queryObject = JsonObject.create().put("match_none", JsonObject.create());
				break;
			case 'B':
				String[] minmax = terms[1].split(":");
				queryObject = SearchQuery.numericRange().max(Double.parseDouble(minmax[0]), true)
						.min(Double.parseDouble(minmax[1]), true).field(secondfieldName).export();
				break;
			case 'C':
				queryObject= SearchQuery.term(terms[0]).field(secondfieldName).export();
				break;
			default:
				queryObject = JsonObject.create().put("match_none", JsonObject.create());
				break;
		}
		queryObject.removeKey("query");
		return queryObject;
	}

	private VectorSearchQuery buildVectorSearchQuery(String[] terms, String fieldName, char caseType) {
		JsonArray vectorArray = JsonArray.create();
		JsonObject queryObject = buildQueryObjectForVectorSearch(terms, fieldName, caseType);
		for (int i = 2; i < terms.length; i = i + 1) {
			BigDecimal vector = BigDecimal.valueOf(Double.parseDouble(terms[i]));
			vectorArray.add(vector);
		}
		return new VectorSearchQuery(vectorArray, k_nearest_neighbour).field(fieldName).queryObject(queryObject);
		}

	private VectorSearchQuery buildVectorBase64SearchQuery(String[] terms, String fieldName, char caseType) {
			
			JsonObject queryObject = buildQueryObjectForVectorSearch(terms, fieldName, caseType);
			String vectorBase64String = terms[2];
			return new VectorSearchQuery(vectorBase64String, k_nearest_neighbour).field(fieldName).queryObject(queryObject);
			}
	
	private SearchQuery buildAndQueryForNumber(String[] terms, String fieldName) {
			TermQuery lt = SearchQuery.term(terms[0]).field(fieldName);
			NumericRangeQuery rt = SearchQuery.numericRange().max(Double.parseDouble(terms[1]), true).min(Double.parseDouble(terms[1]), true).field(secondfieldName);
			return SearchQuery.conjuncts(lt, rt);
		}

	private SearchQuery buildAndQueryForNumberAndNumber(String[] terms, String fieldName) {
			NumericRangeQuery lt = SearchQuery.numericRange().max(Double.parseDouble(terms[0]), true).min(Double.parseDouble(terms[0]), true).field(fieldName);
			NumericRangeQuery rt = SearchQuery.numericRange().max(Double.parseDouble(terms[1]), true).min(Double.parseDouble(terms[1]), true).field(secondfieldName);
			return SearchQuery.conjuncts(lt, rt);
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
		SearchOptions opt = SearchOptions.searchOptions().limit(limit).timeout(Duration.ofSeconds(Search_Query_timeout));
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
