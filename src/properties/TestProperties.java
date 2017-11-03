package properties;

import java.util.HashMap;
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


    // General test settings
    public static final String TESTSPEC_TEST_DURATION = "test_duration";
    private static final String TESTSPEC_TEST_DURATION_DEFAULT = "600";

    public static final String TESTSPEC_THREADS = "test_threads";
    private static final String TESTSPEC_THREADS_DEFAULT = "10";

    public static final String TESTSPEC_TESTDATA_FILE = "test_data_file";
    private static final String TESTSPEC_TESTDATA_FILE_DEFAULT = "low.txt";

    public static final String TESTSPEC_DATASET_MULTIPLIER = "test_dataset_multiplier";
    private static final String TESTSPEC_DATASET_MULTIPLIER_DEFAULT = "10";

    public static final String TESTSPEC_DRIVER = "test_driver";
    private static final String TESTSPEC_DRIVER_DEFAULT = "couchbase";

    public static final String TESTSPEC_STATS_LIMIT = "test_stats_limit";
    private static final String TESTSPEC_STATS_LIMIT_DEFAULT = "1000000";

    public static final String TESTSPEC_STATS_AGGR_STEP = "test_stats_aggregation_step";
    private static final String TESTSPEC_STATS_AGGR_STEP_DEFAULT = "1000";

    public static final String TESTSPEC_TEST_DEBUGMODE = "test_debug";
    private static final String TESTSPEC_TEST_DEBUGMODE_DEFAULT = "true";

    // Couchbase-specific settings
    public static final String CBSPEC_INDEX_NAME = "couchbase_index_name";
    private static final String CBSPEC_INDEX_NAME_DEFAILT = "fts_perf";

    public static final String CBSPEC_QUERY_TYPE = "couchbase_query_type";
    private static final String CBSPEC_QUERY_TYPE_DEFAULT = "term";

    public static final String CBSPEC_SERVER = "couchbase_servers_list";
    private static final String CBSPEC_SERVER_DEFAULT = "localhost";

    public static final String CBSPEC_CBBUCKET = "couchbase_bucket";
    private static final String CBSPEC_CBBUCKET_DEFAULT = "bucket-1";

    public static final String CBSPEC_USER = "couchbase_user";
    private static final String CBSPEC_USER_DEFAULT = "Administrator";

    public static final String CBSPEC_PASSWORD = "couchbase_password";
    private static final String CBSPEC_PASSWORD_DEFAULT = "password";



    private HashMap<String, String> prop = new HashMap<>();

    private HashMap<String, String> driversMapping = new HashMap<>();

    public TestProperties(String[] args) {

        driversMapping.put("couchbase", "drivers.CouchbaseClient");
        driversMapping.put("couchbase-sdk", "drivers.CouchbaseClient");
        driversMapping.put("couchbase-rest", "drivers.CouchbaseClientREST");
        driversMapping.put("mongodb", "drivers.MongodbClient");
        driversMapping.put("elasticsearch", "drivers.ElasticClient");
        driversMapping.put("elastic", "drivers.ElasticClient");

        Options options = new Options();
        options.addOption(new Option(TESTSPEC_TEST_DURATION, ""));
        options.addOption(new Option(TESTSPEC_THREADS, ""));
        options.addOption(new Option(TESTSPEC_TESTDATA_FILE, ""));
        options.addOption(new Option(TESTSPEC_DATASET_MULTIPLIER, ""));
        options.addOption(new Option(TESTSPEC_DRIVER, ""));
        options.addOption(new Option(TESTSPEC_STATS_LIMIT, ""));
        options.addOption(new Option(TESTSPEC_STATS_AGGR_STEP, ""));
        options.addOption(new Option(TESTSPEC_TEST_DEBUGMODE, ""));

        options.addOption(new Option(CBSPEC_INDEX_NAME, ""));
        options.addOption(new Option(CBSPEC_QUERY_TYPE, ""));
        options.addOption(new Option(CBSPEC_SERVER, ""));
        options.addOption(new Option(CBSPEC_CBBUCKET, ""));
        options.addOption(new Option(CBSPEC_USER, ""));
        options.addOption(new Option(CBSPEC_PASSWORD, ""));

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("fts-javagen", options);
            System.exit(1);
            return;
        }

        prop.put(TESTSPEC_TEST_DURATION, cmd.getOptionValue(TESTSPEC_TEST_DURATION, TESTSPEC_TEST_DURATION_DEFAULT));
        prop.put(TESTSPEC_THREADS, cmd.getOptionValue(TESTSPEC_THREADS, TESTSPEC_THREADS_DEFAULT));
        prop.put(TESTSPEC_TEST_DEBUGMODE, cmd.getOptionValue(TESTSPEC_TEST_DEBUGMODE, TESTSPEC_TEST_DEBUGMODE_DEFAULT));
        prop.put(TESTSPEC_TESTDATA_FILE, cmd.getOptionValue(TESTSPEC_TESTDATA_FILE, TESTSPEC_TESTDATA_FILE_DEFAULT));
        prop.put(TESTSPEC_DATASET_MULTIPLIER, cmd.getOptionValue(TESTSPEC_DATASET_MULTIPLIER,
                TESTSPEC_DATASET_MULTIPLIER_DEFAULT));
        prop.put(TESTSPEC_DRIVER, getDriverClassName(cmd.getOptionValue(TESTSPEC_DRIVER, TESTSPEC_DRIVER_DEFAULT)));
        prop.put(TESTSPEC_STATS_LIMIT, cmd.getOptionValue(TESTSPEC_STATS_LIMIT, TESTSPEC_STATS_LIMIT_DEFAULT));
        prop.put(TESTSPEC_STATS_AGGR_STEP, cmd.getOptionValue(TESTSPEC_STATS_AGGR_STEP,
                TESTSPEC_STATS_AGGR_STEP_DEFAULT));

        prop.put(CBSPEC_INDEX_NAME, cmd.getOptionValue(CBSPEC_INDEX_NAME, CBSPEC_INDEX_NAME_DEFAILT));
        prop.put(CBSPEC_QUERY_TYPE, cmd.getOptionValue(CBSPEC_QUERY_TYPE, CBSPEC_QUERY_TYPE_DEFAULT));
        prop.put(CBSPEC_SERVER, cmd.getOptionValue(CBSPEC_SERVER, CBSPEC_SERVER_DEFAULT));
        prop.put(CBSPEC_CBBUCKET, cmd.getOptionValue(CBSPEC_CBBUCKET, CBSPEC_CBBUCKET_DEFAULT));
        prop.put(CBSPEC_USER, cmd.getOptionValue(CBSPEC_USER, CBSPEC_USER_DEFAULT));
        prop.put(CBSPEC_PASSWORD, cmd.getOptionValue(CBSPEC_PASSWORD, CBSPEC_PASSWORD_DEFAULT));


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
