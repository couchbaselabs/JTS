package com.couchbase.jts.properties;

import java.util.HashMap;
import java.util.UUID;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

/**
 * Created by oleksandr.gyryk on 10/3/17.
 */
public class TestProperties {

    //static const
    public static final String CONSTANT_QUERY_TYPE_TERM = "term";
    public static final String CONSTANT_QUERY_TYPE_AND = "2_conjuncts"; //(term1 AND term2)
    public static final String CONSTANT_QUERY_TYPE_OR = "2_disjuncts"; // (term1 OR term2)
    public static final String CONSTANT_QUERY_TYPE_AND_OR_OR = "1_conjuncts_2_disjuncts"; //(term1 AND (term2 OR term3))
    public static final String CONSTANT_QUERY_TYPE_FUZZY = "fuzzy";
    public static final String CONSTANT_QUERY_TYPE_PHRASE = "match_phrase";
    public static final String CONSTANT_QUERY_TYPE_PREFIX = "prefix";
    public static final String CONSTANT_QUERY_TYPE_WILDCARD = "wildcard";
    public static final String CONSTANT_QUERY_TYPE_FACET = "facet";
    public static final String CONSTANT_QUERY_TYPE_NUMERIC = "numeric";
    public static final String CONSTANT_QUERY_TYPE_DATERANGE = "daterange";
    public static final String CONSTANT_QUERY_TYPE_GEO_RADIUS	= "geo_rad";
    public static final String CONSTANT_QUERY_TYPE_GEO_BOX ="geo_box";
    public static final String CONSTANT_QUERY_TYPE_GEO_POLYGON = "geo_polygon";
    public static final String CONSTANT_QUERY_TYPE_MATCH = "match";
    public static final String CONSTANT_QUERY_TYPE_GEOSHAPE = "geo_shape";
    public static final String CONSTANT_QUERY_TYPE_VECTOR = "vector";
    public static final String CONSTANT_QUERY_TYPE_MULTIPLE_VECTOR = "multi_vector";
    public static final String CONSTANT_QUERY_TYPE_TEXT_VECTOR = "text_vector";
    public static final String CONSTANT_QUERY_TYPE_NUMERIC_VECTOR = "numeric_vector";
    public static final String CONSTANT_QUERY_TYPE_BASE64_VECTOR = "vector_base64";

    //Flex query types
    public static final String CONSTANT_FLEX_QUERY_TYPE_ARRAY = "array_predicate";
    public static final String CONSTANT_FLEX_QUERY_TYPE_MIXED1 = "mixed1";
    public static final String CONSTANT_FLEX_QUERY_TYPE_MIXED2 = "mixed2";
    public static final String CONSTANT_JTS_LOG_DIR = UUID.randomUUID().toString();


    // Test settings
    public static final String TESTSPEC_TEST_DURATION = "test_duration";
    private static final String TESTSPEC_TEST_DURATION_DEFAULT = "10";

    public static final String TESTSPEC_TOTAL_DOCS = "test_total_docs";
    private static final String TESTSPEC_TOTAL_DOCS_DEFAULT = "1000000";

    public static final String TESTSPEC_QUERY_WORKERS = "test_query_workers";
    private static final String TESTSPEC_QUERY_WORKERS_DEFAULT = "1";

    public static final String TESTSPEC_KV_WORKERS = "test_kv_workers";
    private static final String TESTSPEC_KV_WORKERS_DEFAULT = "0";

    public static final String TESTSPEC_KV_THROUGHPUT_GOAL = "test_kv_throughput_goal";
    private static final String TESTSPEC_KV_THROUGHPUT_GOAL_DEFAULT = "1000";

    public static final String TESTSPEC_TESTDATA_FILE = "test_data_file";
    private static final String TESTSPEC_TESTDATA_FILE_DEFAULT = "/res/mdb/low.txt";

    public static final String TESTSPEC_DRIVER = "test_driver";
    private static final String TESTSPEC_DRIVER_DEFAULT = "couchbase";

    public static final String TESTSPEC_STATS_LIMIT = "test_stats_limit";
    private static final String TESTSPEC_STATS_LIMIT_DEFAULT = "100000";

    public static final String TESTSPEC_STATS_AGGR_STEP = "test_stats_aggregation_step";
    private static final String TESTSPEC_STATS_AGGR_STEP_DEFAULT = "1000";

    public static final String TESTSPEC_TEST_DEBUGMODE = "test_debug";
    private static final String TESTSPEC_TEST_DEBUGMODE_DEFAULT = "true";

    public static final String TESTSPEC_QUERY_TYPE = "test_query_type";
    private static final String TESTSPEC_QUERY_TYPE_DEFAULT = "term";

    public static final String TESTSPEC_VECTOR_FILTER_ENABLED = "test_vector_enabled";
    private static final String TESTSPEC_VECTOR_FILTER_ENABLED_DEFAULT = "false";

    public static final String TESTSPEC_VECTOR_FILTER_MIN_MAX = "test_vector_filter_minmax";
    private static final String TESTSPEC_VECTOR_FILTER_MIN_MAX_DEFAULT = "0:0";

    public static final String TESTSPEC_VECTOR_FILTER_MATCH = "test_vector_filter_match";
    private static final String TESTSPEC_VECTOR_FILTER_MATCH_DEFAULT = "";

    public static final String TESTSPEC_GEO_DISTANCE = "test_geo_distance";
    private static final String TESTSPEC_GEO_DISTANCE_DEFAULT = "10mi";

    public static final String TESTSPEC_GEO_LAT_HEIGHT = "test_query_lat_height";
    private static final String TESTSPEC_GEO_LAT_HEIGHT_DEFAULT ="3";

    public static final String TESTSPEC_GEO_LON_WIDTH = "test_query_lon_width";
    private static final String TESTSPEC_GEO_LON_WIDTH_DEFAULT = "4";

    public static final String TESTSPEC_GEO_POLYGON_COORD_LIST = "test_geo_polygon_coord_list";
    private static final String TESTSPEC_GEO_POLYGON_COORD_LIST_DEFAULT = "";

    public static final String TESTSPEC_QUERY_LIMIT = "test_query_limit";
    private static final String TESTSPEC_QUERY_LIMIT_DEFAULT = "10";

    public static final String TESTSPEC_QUERY_FIELD = "test_query_field";
    private static final String TESTSPEC_QUERY_FIELD_DEFAULT = "text";

    public static final String TESTSPEC_QUERY_FIELD2 = "test_query_field2";
    private static final String TESTSPEC_QUERY_FIELD2_DEFAULT = "text";

    public static final String TESTSPEC_MUTATION_FIELD = "test_mutation_field";
    private static final String TESTSPEC_MUTATION_FIELD_DEFAULT = "false";

    public static final String TESTSPEC_WORKER_TYPE = "test_worker_type";
    private static final String TESTSPEC_WORKER_TYPE_DEFAULT = "latency"; //debug, latency, throughput

    //FLEX Settings one flag and the other for flexQueryType
    public static final String TESTSPEC_FLEX="test_flex";
    private static final String TESTSPEC_FLEX_DEFAULT ="false";

    public static final String TESTSPEC_FLEX_QUERY_TYPE = "test_flex_query_type";
    private static final String TESTSPEC_FLEX_QUERY_TYPE_DEFAULT ="array_predicate";

    // Collections setting
    public static final String TESTSPEC_COLLECTIONS_ENABLED ="test_collections_enabled";
    private static final String TESTSPEC_COLLECTIONS_ENABLED_DEFAULT = "false";

    public static final String TESTSPEC_COLLECTION_QUERY_MODE ="test_collection_query_mode";
    private static final String TESTSPEC_COLLECTION_QUERY_MODE_DEFAULT = "default";

    public static final String TESTSPEC_COLLECTION_SPECIFIC_COUNT ="test_collection_specific_count";
    private static final String TESTSPEC_COLLECTION_SPECIFIC_COUNT_DEFAULT = "1";

    public static final String TESTSPEC_FTS_INDEX_MAP ="test_fts_index_map";
    private static final String TESTSPEC_FTS_INDEX_MAP_DEFAULT = "";

    // Couchbase-specific settings
    public static final String CBSPEC_INDEX_NAME = "couchbase_index_name";
    private static final String CBSPEC_INDEX_NAME_DEFAILT = "perf_fts_index";

    public static final String CBSPEC_SERVER = "couchbase_cluster_ip";
    private static final String CBSPEC_SERVER_DEFAULT = "172.23.99.211";

    public static final String CBSPEC_CBBUCKET = "couchbase_bucket";
    private static final String CBSPEC_CBBUCKET_DEFAULT = "bucket-1";

    public static final String CBSPEC_USER = "couchbase_user";
    private static final String CBSPEC_USER_DEFAULT = "Administrator";

    public static final String CBSPEC_PASSWORD = "couchbase_password";
    private static final String CBSPEC_PASSWORD_DEFAULT = "password";

    public static final String CBSPEC_SSLMODE = "couchbase_ssl_mode";
    private static final String CBSPEC_SSLMODE_DEFAULT = "none";

    public static final String SEARCH_QUERY_TIMEOUT_IN_SEC = "search_query_timeout_in_sec";
    public static final String SEARCH_QUERY_TIMEOUT_IN_SEC_DEFAULT = "10";

    public static final String AGGREGATION_BUFFER_MS = "aggregation_buffer_ms";
    public static final String AGGREGATION_BUFFER_MS_DEFAULT = "1000";


    // Mongodb-specific settings
    public static final String MDBSPEC_INDEX_NAME = "mongodb_index_name";
    private static final String MDBSPEC_INDEX_NAME_DEFAILT = "text_index";

    public static final String MDBSPEC_SERVER = "mongodb_mongos_ip";
    private static final String MDBSPEC_SERVER_DEFAULT = "172.23.99.210";

    public static final String MDBSPEC_PORT = "mongodb_mongos_port";
    private static final String MDBSPEC_PORT_DEFAULT = "27021";

    public static final String MDBSPEC_DATABASE = "mongodb_database";
    private static final String MDBSPEC_DATABASE_DEFAULT = "wiki";

    public static final String MDBSPEC_COLLECTION = "mongodb_collection";
    private static final String MDBSPEC_COLLECTION_DEFAULT = "bucket1";

    public static final String TESTSPEC_GEOJSON_QUERY_TYPE = "test_geojson_query_type";
    private static final String TESTSPEC_GEOJSON_QUERY_TYPE_DEFAULT = "point";

    public static final String TESTSPEC_GEOJSON_QUERY_RELATION = "test_geojson_query_relation";
    private static final String TESTSPEC_GEOJSON_QUERY_RELATION_DEFAULT = "contains";

    public static final String TESTSPEC_FTS_RAW_QUERY_MAP = "fts_raw_query_map";
    private static final String TESTSPEC_FTS_RAW_QUERY_MAP_DEFAULT = "";
    
    public static final String K_NEAREST_NEIGHBOUR = "k_nearest_neighbour";
    private static final String K_NEAREST_NEIGHBOUR_DEFAULT = "3";

    private HashMap<String, String> prop = new HashMap<>();

    private HashMap<String, String> driversMapping = new HashMap<>();

    public TestProperties(String[] args) {

        driversMapping.put("couchbase", "com.couchbase.jts.drivers.CouchbaseClient");
        driversMapping.put("couchbase-sdk", "couchbase.jts.drivers.CouchbaseClient");
        driversMapping.put("mongodb", "com.couchbase.jts.drivers.MongodbClient");
        driversMapping.put("elasticsearch", "com.couchbase.jts.drivers.ElasticClient");
        driversMapping.put("elastic", "com.couchbase.jts.drivers.ElasticClient");

        Options options = new Options();

        options.addOption(Option.builder(TESTSPEC_TEST_DURATION).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_TOTAL_DOCS).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_KV_WORKERS).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_KV_THROUGHPUT_GOAL).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_QUERY_WORKERS).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_TESTDATA_FILE).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_DRIVER).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_STATS_LIMIT).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_STATS_AGGR_STEP).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_TEST_DEBUGMODE).hasArg().required(false).build());

        // Query type and QueryOptions related parameters
        options.addOption(Option.builder(TESTSPEC_QUERY_TYPE).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_VECTOR_FILTER_ENABLED).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_VECTOR_FILTER_MIN_MAX).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_VECTOR_FILTER_MATCH).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_QUERY_LIMIT).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_QUERY_FIELD).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_QUERY_FIELD2).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_MUTATION_FIELD).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_WORKER_TYPE).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_FTS_RAW_QUERY_MAP).hasArg().required(false).build());
        options.addOption(Option.builder(K_NEAREST_NEIGHBOUR).hasArg().required(false).build());


        // Additional parameters for geo queries
        options.addOption(Option.builder(TESTSPEC_GEO_DISTANCE).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_GEO_LAT_HEIGHT).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_GEO_LON_WIDTH).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_GEO_POLYGON_COORD_LIST).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_GEOJSON_QUERY_TYPE).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_GEOJSON_QUERY_RELATION).hasArg().required(false).build());


        // Additional parameters for Flex
        options.addOption(Option.builder(TESTSPEC_FLEX).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_FLEX_QUERY_TYPE).hasArg().required(false).build());

        // Additional flags for Collections
        options.addOption(Option.builder(TESTSPEC_COLLECTIONS_ENABLED).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_COLLECTION_QUERY_MODE).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_COLLECTION_SPECIFIC_COUNT).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_FTS_INDEX_MAP).hasArg().required(false).build());

        // Couchbase authentication related parameters
        options.addOption(Option.builder(CBSPEC_INDEX_NAME).hasArg().required(false).build());
        options.addOption(Option.builder(CBSPEC_SERVER).hasArg().required(false).build());
        options.addOption(Option.builder(CBSPEC_CBBUCKET).hasArg().required(false).build());
        options.addOption(Option.builder(CBSPEC_USER).hasArg().required(false).build());
        options.addOption(Option.builder(CBSPEC_PASSWORD).hasArg().required(false).build());
        options.addOption(Option.builder(CBSPEC_SSLMODE).hasArg().required(false).build());
        options.addOption(Option.builder(SEARCH_QUERY_TIMEOUT_IN_SEC).hasArg().required(false).build());
        options.addOption(Option.builder(AGGREGATION_BUFFER_MS).hasArg().required(false).build());


        options.addOption(Option.builder(MDBSPEC_INDEX_NAME).hasArg().required(false).build());
        options.addOption(Option.builder(MDBSPEC_SERVER).hasArg().required(false).build());
        options.addOption(Option.builder(MDBSPEC_PORT).hasArg().required(false).build());
        options.addOption(Option.builder(MDBSPEC_DATABASE).hasArg().required(false).build());
        options.addOption(Option.builder(MDBSPEC_COLLECTION).hasArg().required(false).build());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("JTS", options);
            System.exit(1);
            return;
        }

        prop.put(TESTSPEC_TEST_DURATION, cmd.getOptionValue(TESTSPEC_TEST_DURATION, TESTSPEC_TEST_DURATION_DEFAULT));
        prop.put(TESTSPEC_TOTAL_DOCS, cmd.getOptionValue(TESTSPEC_TOTAL_DOCS, TESTSPEC_TOTAL_DOCS_DEFAULT));
        prop.put(TESTSPEC_KV_WORKERS, cmd.getOptionValue(TESTSPEC_KV_WORKERS, TESTSPEC_KV_WORKERS_DEFAULT));
        prop.put(TESTSPEC_KV_THROUGHPUT_GOAL, cmd.getOptionValue(TESTSPEC_KV_THROUGHPUT_GOAL,
                TESTSPEC_KV_THROUGHPUT_GOAL_DEFAULT));
        prop.put(TESTSPEC_QUERY_WORKERS, cmd.getOptionValue(TESTSPEC_QUERY_WORKERS, TESTSPEC_QUERY_WORKERS_DEFAULT));
        prop.put(TESTSPEC_TEST_DEBUGMODE, cmd.getOptionValue(TESTSPEC_TEST_DEBUGMODE, TESTSPEC_TEST_DEBUGMODE_DEFAULT));
        prop.put(TESTSPEC_TESTDATA_FILE, cmd.getOptionValue(TESTSPEC_TESTDATA_FILE, TESTSPEC_TESTDATA_FILE_DEFAULT));
        prop.put(TESTSPEC_DRIVER, getDriverClassName(cmd.getOptionValue(TESTSPEC_DRIVER, TESTSPEC_DRIVER_DEFAULT)));
        prop.put(TESTSPEC_STATS_LIMIT, cmd.getOptionValue(TESTSPEC_STATS_LIMIT, TESTSPEC_STATS_LIMIT_DEFAULT));
        prop.put(TESTSPEC_STATS_AGGR_STEP, cmd.getOptionValue(TESTSPEC_STATS_AGGR_STEP,
                TESTSPEC_STATS_AGGR_STEP_DEFAULT));
        prop.put(TESTSPEC_QUERY_TYPE, cmd.getOptionValue(TESTSPEC_QUERY_TYPE, TESTSPEC_QUERY_TYPE_DEFAULT));
        prop.put(TESTSPEC_VECTOR_FILTER_ENABLED, cmd.getOptionValue(TESTSPEC_VECTOR_FILTER_ENABLED, TESTSPEC_VECTOR_FILTER_ENABLED_DEFAULT));
        prop.put(TESTSPEC_VECTOR_FILTER_MATCH, cmd.getOptionValue(TESTSPEC_VECTOR_FILTER_MATCH, TESTSPEC_VECTOR_FILTER_MATCH_DEFAULT));
        prop.put(TESTSPEC_VECTOR_FILTER_MIN_MAX, cmd.getOptionValue(TESTSPEC_VECTOR_FILTER_MIN_MAX, TESTSPEC_VECTOR_FILTER_MIN_MAX_DEFAULT));
        prop.put(TESTSPEC_QUERY_LIMIT, cmd.getOptionValue(TESTSPEC_QUERY_LIMIT, TESTSPEC_QUERY_LIMIT_DEFAULT));
        prop.put(TESTSPEC_QUERY_FIELD, cmd.getOptionValue(TESTSPEC_QUERY_FIELD, TESTSPEC_QUERY_FIELD_DEFAULT));
        prop.put(TESTSPEC_QUERY_FIELD2, cmd.getOptionValue(TESTSPEC_QUERY_FIELD2, TESTSPEC_QUERY_FIELD2_DEFAULT));
        prop.put(TESTSPEC_MUTATION_FIELD, cmd.getOptionValue(TESTSPEC_MUTATION_FIELD, TESTSPEC_MUTATION_FIELD_DEFAULT));
        prop.put(TESTSPEC_WORKER_TYPE, cmd.getOptionValue(TESTSPEC_WORKER_TYPE, TESTSPEC_WORKER_TYPE_DEFAULT));
        prop.put(TESTSPEC_FTS_RAW_QUERY_MAP, cmd.getOptionValue(TESTSPEC_FTS_RAW_QUERY_MAP, TESTSPEC_FTS_RAW_QUERY_MAP_DEFAULT));
        prop.put(K_NEAREST_NEIGHBOUR, cmd.getOptionValue(K_NEAREST_NEIGHBOUR, K_NEAREST_NEIGHBOUR_DEFAULT));


        // Additional Collection flags
        prop.put(TESTSPEC_COLLECTIONS_ENABLED, cmd.getOptionValue(TESTSPEC_COLLECTIONS_ENABLED, TESTSPEC_COLLECTIONS_ENABLED_DEFAULT));
        prop.put(TESTSPEC_COLLECTION_QUERY_MODE, cmd.getOptionValue(TESTSPEC_COLLECTION_QUERY_MODE, TESTSPEC_COLLECTION_QUERY_MODE_DEFAULT));
        prop.put(TESTSPEC_COLLECTION_SPECIFIC_COUNT, cmd.getOptionValue(TESTSPEC_COLLECTION_SPECIFIC_COUNT, TESTSPEC_COLLECTION_SPECIFIC_COUNT_DEFAULT));
        prop.put(TESTSPEC_FTS_INDEX_MAP, cmd.getOptionValue(TESTSPEC_FTS_INDEX_MAP, TESTSPEC_FTS_INDEX_MAP_DEFAULT));

        // Additional Flex queries parameter
        prop.put(TESTSPEC_FLEX, cmd.getOptionValue(TESTSPEC_FLEX,TESTSPEC_FLEX_DEFAULT));
        prop.put(TESTSPEC_FLEX_QUERY_TYPE,cmd.getOptionValue(TESTSPEC_FLEX_QUERY_TYPE,TESTSPEC_FLEX_QUERY_TYPE_DEFAULT));

        // Additional Geo Queries parameter
        prop.put(TESTSPEC_GEO_DISTANCE, cmd.getOptionValue(TESTSPEC_GEO_DISTANCE,TESTSPEC_GEO_DISTANCE_DEFAULT));
        prop.put(TESTSPEC_GEO_LAT_HEIGHT,cmd.getOptionValue( TESTSPEC_GEO_LAT_HEIGHT,TESTSPEC_GEO_LAT_HEIGHT_DEFAULT ));
        prop.put(TESTSPEC_GEO_POLYGON_COORD_LIST, cmd.getOptionValue(TESTSPEC_GEO_POLYGON_COORD_LIST,TESTSPEC_GEO_POLYGON_COORD_LIST_DEFAULT));
        prop.put(TESTSPEC_GEO_LON_WIDTH,cmd.getOptionValue(TESTSPEC_GEO_LON_WIDTH,TESTSPEC_GEO_LON_WIDTH_DEFAULT));
        prop.put(TESTSPEC_GEOJSON_QUERY_TYPE,cmd.getOptionValue(TESTSPEC_GEOJSON_QUERY_TYPE,TESTSPEC_GEOJSON_QUERY_TYPE_DEFAULT));
        prop.put(TESTSPEC_GEOJSON_QUERY_RELATION,cmd.getOptionValue(TESTSPEC_GEOJSON_QUERY_RELATION,TESTSPEC_GEOJSON_QUERY_RELATION_DEFAULT));


        prop.put(CBSPEC_INDEX_NAME, cmd.getOptionValue(CBSPEC_INDEX_NAME, CBSPEC_INDEX_NAME_DEFAILT));
        prop.put(CBSPEC_SERVER, cmd.getOptionValue(CBSPEC_SERVER, CBSPEC_SERVER_DEFAULT));
        prop.put(CBSPEC_CBBUCKET, cmd.getOptionValue(CBSPEC_CBBUCKET, CBSPEC_CBBUCKET_DEFAULT));
        prop.put(CBSPEC_USER, cmd.getOptionValue(CBSPEC_USER, CBSPEC_USER_DEFAULT));
        prop.put(CBSPEC_PASSWORD, cmd.getOptionValue(CBSPEC_PASSWORD, CBSPEC_PASSWORD_DEFAULT));
        prop.put(CBSPEC_SSLMODE, cmd.getOptionValue(CBSPEC_SSLMODE, CBSPEC_SSLMODE_DEFAULT));
        prop.put(SEARCH_QUERY_TIMEOUT_IN_SEC, cmd.getOptionValue(SEARCH_QUERY_TIMEOUT_IN_SEC, SEARCH_QUERY_TIMEOUT_IN_SEC_DEFAULT));
        prop.put(AGGREGATION_BUFFER_MS, cmd.getOptionValue(AGGREGATION_BUFFER_MS, AGGREGATION_BUFFER_MS_DEFAULT));


        prop.put(MDBSPEC_INDEX_NAME, cmd.getOptionValue(MDBSPEC_INDEX_NAME, MDBSPEC_INDEX_NAME_DEFAILT));
        prop.put(MDBSPEC_SERVER, cmd.getOptionValue(MDBSPEC_SERVER, MDBSPEC_SERVER_DEFAULT));
        prop.put(MDBSPEC_PORT, cmd.getOptionValue(MDBSPEC_PORT, MDBSPEC_PORT_DEFAULT));
        prop.put(MDBSPEC_DATABASE, cmd.getOptionValue(MDBSPEC_DATABASE, MDBSPEC_DATABASE_DEFAULT));
        prop.put(MDBSPEC_COLLECTION, cmd.getOptionValue(MDBSPEC_COLLECTION, MDBSPEC_COLLECTION_DEFAULT));
    }

    public String getDriverClassName(String driverName) {
        if (driversMapping.containsKey(driverName)) {
            return driversMapping.get(driverName);
        }
        return null;
    }

    public boolean isDebugMode(){
        return prop.get(TESTSPEC_TEST_DEBUGMODE).equals("true");
    }

    public String get(String key) {
        return prop.get(key);
    }

    public String getAllPropsAsString() {
        String allProps = "";
        for (String name: prop.keySet()){
            String key = name.toString();
            String value = prop.get(name).toString();
            allProps = allProps + key + " = " + value + '\n';
        }
        return allProps;
    }


}
