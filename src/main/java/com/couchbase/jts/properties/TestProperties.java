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
    public static final String CONSTANT_QUERY_TYPE_GEO_RADIUS	= "geo_rad"; 
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

    public static final String TESTSPEC_GEO_DISTANCE = "test_geo_distance";
    private static final String TESTSPEC_GEO_DISTANCE_DEFAULT = "10";

    public static final String TESTSPEC_QUERY_LIMIT = "test_query_limit";
    private static final String TESTSPEC_QUERY_LIMIT_DEFAULT = "10";

    public static final String TESTSPEC_QUERY_FIELD = "test_query_field";
    private static final String TESTSPEC_QUERY_FIELD_DEFAULT = "text";

    public static final String TESTSPEC_MUTATION_FIELD = "test_mutation_field";
    private static final String TESTSPEC_MUTATION_FIELD_DEFAULT = "text2";

    public static final String TESTSPEC_WORKER_TYPE = "test_worker_type";
    private static final String TESTSPEC_WORKER_TYPE_DEFAULT = "latency";
        //debug, latency, throughput, validatedThroughput


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
        options.addOption(Option.builder(TESTSPEC_QUERY_TYPE).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_GEO_DISTANCE).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_QUERY_LIMIT).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_QUERY_FIELD).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_MUTATION_FIELD).hasArg().required(false).build());
        options.addOption(Option.builder(TESTSPEC_WORKER_TYPE).hasArg().required(false).build());

        options.addOption(Option.builder(CBSPEC_INDEX_NAME).hasArg().required(false).build());
        options.addOption(Option.builder(CBSPEC_SERVER).hasArg().required(false).build());
        options.addOption(Option.builder(CBSPEC_CBBUCKET).hasArg().required(false).build());
        options.addOption(Option.builder(CBSPEC_USER).hasArg().required(false).build());
        options.addOption(Option.builder(CBSPEC_PASSWORD).hasArg().required(false).build());

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
        prop.put(TESTSPEC_QUERY_LIMIT, cmd.getOptionValue(TESTSPEC_QUERY_LIMIT, TESTSPEC_QUERY_LIMIT_DEFAULT));
        prop.put(TESTSPEC_QUERY_FIELD, cmd.getOptionValue(TESTSPEC_QUERY_FIELD, TESTSPEC_QUERY_FIELD_DEFAULT));
        prop.put(TESTSPEC_MUTATION_FIELD, cmd.getOptionValue(TESTSPEC_MUTATION_FIELD, TESTSPEC_MUTATION_FIELD_DEFAULT));
        prop.put(TESTSPEC_WORKER_TYPE, cmd.getOptionValue(TESTSPEC_WORKER_TYPE, TESTSPEC_WORKER_TYPE_DEFAULT));
        prop.put(TESTSPEC_GEO_DISTANCE, cmd.getOptionValue(TESTSPEC_GEO_DISTANCE,TESTSPEC_GEO_DISTANCE_DEFAULT));
        prop.put(CBSPEC_INDEX_NAME, cmd.getOptionValue(CBSPEC_INDEX_NAME, CBSPEC_INDEX_NAME_DEFAILT));
        prop.put(CBSPEC_SERVER, cmd.getOptionValue(CBSPEC_SERVER, CBSPEC_SERVER_DEFAULT));
        prop.put(CBSPEC_CBBUCKET, cmd.getOptionValue(CBSPEC_CBBUCKET, CBSPEC_CBBUCKET_DEFAULT));
        prop.put(CBSPEC_USER, cmd.getOptionValue(CBSPEC_USER, CBSPEC_USER_DEFAULT));
        prop.put(CBSPEC_PASSWORD, cmd.getOptionValue(CBSPEC_PASSWORD, CBSPEC_PASSWORD_DEFAULT));

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
