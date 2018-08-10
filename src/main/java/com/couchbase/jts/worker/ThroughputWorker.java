package com.couchbase.jts.worker;

import com.couchbase.jts.drivers.Client;
import com.couchbase.jts.logger.ThroughputLogger;

import java.io.IOException;

/**
 * Created by oleksandr.gyryk on 11/30/17.
 */
public class ThroughputWorker extends Worker{

    protected ThroughputLogger throughputLogger;

    public ThroughputWorker(Client client, int workerId){
        super(client);
        throughputLogger = new ThroughputLogger(statsLimit, workerId);
    }

    public void runAction(){
        clientDB.query();
        throughputLogger.logRequest();
    }

    public void shutDown() {
        try {
            throughputLogger.dumpThroughput();
        } catch (IOException e) {
            System.err.println("ERROR Failed to dump throughput stats to disk: " + e.getMessage());
        }
    }

}
