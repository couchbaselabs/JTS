package worker;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.search.queries.*;
import drivers.Client;
import logger.StatusLogger;
import logger.LatencyLogger;
import logger.ThroughputLogger;
import properties.TestProperties;


import java.util.List;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public class Worker implements Runnable{

    private LatencyLogger latencyLogger;
    private ThroughputLogger throughputLogger;
    private StatusLogger statusLogger;

    private int requestsCounter;
    private List<AbstractFtsQuery> queries;
    private Bucket bucket;
    private int id;

    public Worker(int workerId, Client client) {
        TestProperties workload = client.getWorkload();
        int statsLimit = Integer.parseInt(workload.get(TestProperties.TESTSPEC_STATS_LIMIT));
        int aggregationStep = Integer.parseInt(workload.get(TestProperties.TESTSPEC_STATS_AGGR_STEP));

        latencyLogger = new LatencyLogger(statsLimit, workerId);
        throughputLogger = new ThroughputLogger(statsLimit, workerId, aggregationStep);
        statusLogger = new StatusLogger("worker_" + workerId + "_status.log", workload.isDebugMode());

        statusLogger.logMessage("worker " + workerId + " initilized");
        id = workerId;
    }

    @Override
    public void run(){
        statusLogger.logMessage("worker " + id + " started");
    }


    private void shutDown() {
        statusLogger.logMessage("Completed, shutting down the worker");

        try {
            latencyLogger.dump();
        } catch (Exception e) {
            statusLogger.logMessage("ERROR Failed to dump latency stats to disk: " + e.getMessage());
        }

        try {
            throughputLogger.dump();
        } catch (Exception e) {
            statusLogger.logMessage("ERROR Failed to dump throughput stats to disk: " + e.getMessage());
        }

        bucket.close();
    }

}
