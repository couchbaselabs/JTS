package main.java.worker;

import main.java.drivers.Client;
import main.java.logger.ThroughputLogger;

import java.io.IOException;

/**
 * Created by oleksandr.gyryk on 11/30/17.
 */
public class ThroughputWorker extends Worker{

    private ThroughputLogger throughputLogger;

    public ThroughputWorker(Client client, int workerId){
        super(client);
        throughputLogger = new ThroughputLogger(statsLimit, workerId);
    }

    public void runQuery(){
        clientDB.query();
        throughputLogger.logRequest();
    }

    public void shutDown() {
        try {
            throughputLogger.dump();
        } catch (IOException e) {
            System.err.println("ERROR Failed to dump throughput stats to disk: " + e.getMessage());
        }
    }

}