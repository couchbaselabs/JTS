package worker;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.search.queries.*;
import drivers.Client;
import logger.StatusLogger;
import logger.LatencyLogger;
import logger.ThroughputLogger;
import properties.TestProperties;


import java.io.IOException;
import java.util.List;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public abstract class Worker implements Runnable{

    protected LatencyLogger latencyLogger;
    protected ThroughputLogger throughputLogger;
    protected StatusLogger statusLogger;

    private int requestsCounter;
    private List<AbstractFtsQuery> queries;
    private Bucket bucket;
    private int id;
    protected Client clientDB;
    private volatile Criteria testCompletedCriteria = new Criteria(false);
    private TestProperties workload;


    public Worker(int workerId, Client client) {
        workload = client.getWorkload();
        int statsLimit = Integer.parseInt(workload.get(TestProperties.TESTSPEC_STATS_LIMIT));
        int aggregationStep = Integer.parseInt(workload.get(TestProperties.TESTSPEC_STATS_AGGR_STEP));

        latencyLogger = new LatencyLogger(statsLimit, workerId);
        throughputLogger = new ThroughputLogger(statsLimit, workerId, aggregationStep);
        statusLogger = new StatusLogger("worker_" + workerId + "_status.log", workload.isDebugMode());
        clientDB = client;

        statusLogger.logMessage("worker " + workerId + " initilized");
        id = workerId;

    }

    @Override
    public void run(){

        long queryCounter = 0;
        testCompletedCriteria.setIsSatisfied(false);
        int testDuration = Integer.parseInt(workload.get(TestProperties.TESTSPEC_TEST_DURATION));
        Thread timer = new Thread(new Timer(testDuration, testCompletedCriteria));
        timer.start();

        while (true) {
            if (testCompletedCriteria.isSatisfied()) {
                shutDown();
                break;
            }
            runQuery();
        }
    }

    abstract protected void runQuery();

    private void shutDown() {
        statusLogger.logMessage("Completed, shutting down the worker");
        try {
            latencyLogger.dump();
        } catch (IOException e) {
            statusLogger.logMessage("ERROR Failed to dump latency stats to disk: " + e.getMessage());
        }

        try {
            throughputLogger.dump();
        } catch (IOException e) {
            statusLogger.logMessage("ERROR Failed to dump throughput stats to disk: " + e.getMessage());
        }
        bucket.close();
    }



    class Criteria {

        private boolean isSatisfied;

        public Criteria(boolean isSatisfied) {
            this.isSatisfied = isSatisfied;
        }

        public boolean isSatisfied() {
            return isSatisfied;
        }

        public void setIsSatisfied(boolean satisfied) {
            this.isSatisfied = satisfied;
        }

    }

    class Timer implements Runnable {

        private long timeout;
        private Criteria timedout;

        public Timer(long timeout, Criteria timedout) {
            this.timedout = timedout;
            this.timeout = timeout;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(timeout);
                this.timedout.setIsSatisfied(true);
            } catch (InterruptedException e) {
                // Do nothing.
            }
        }

    }

}

