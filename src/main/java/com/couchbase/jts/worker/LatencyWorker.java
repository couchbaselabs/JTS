package com.couchbase.jts.worker;

import com.couchbase.jts.drivers.Client;
import com.couchbase.jts.logger.LatencyLogger;
import com.couchbase.jts.logger.ThroughputLogger;

import java.io.IOException;

/**
 * Created by oleksandr.gyryk on 11/30/17.
 */
public class LatencyWorker extends Worker{

    protected LatencyLogger latencyLogger;
    protected ThroughputLogger throughputLogger;

    public LatencyWorker(Client client, int workerId){
        super(client);
        latencyLogger = new LatencyLogger(statsLimit, workerId, workload);
        throughputLogger = new ThroughputLogger(statsLimit, workerId, workload);
    }

    public void runAction(){
        float latencyMS = clientDB.queryAndLatency();
        latencyLogger.logLatency(latencyMS);
        throughputLogger.logRequest();
    }

    public void shutDown() {
        try {
            latencyLogger.dumpLatency();
        } catch (IOException e) {
            System.err.println("ERROR Failed to dump latency stats to disk: " + e.getMessage());
        }

        try {
            throughputLogger.dumpThroughput();
        } catch (IOException e) {
            System.err.println("ERROR Failed to dump throughput stats to disk: " + e.getMessage());
        }
    }


}
