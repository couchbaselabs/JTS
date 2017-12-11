package main.java.worker;

import main.java.drivers.Client;
import main.java.logger.ThroughputLogger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by oleksandr.gyryk on 12/7/17.
 */
public class KVWorker extends Worker {

    private ThroughputLogger throughputLogger;
    private long delay;
    public KVWorker(Client client, int workerId, long delay) {
        super(client);
        this.delay = delay;
        throughputLogger = new ThroughputLogger(statsLimit, workerId, "kv");
    }

    public void runAction(){
        try {
            TimeUnit.MILLISECONDS.sleep(delay);
            clientDB.mutateRandomDoc();
            throughputLogger.logRequest();
        } catch (Exception ex) {
            return;
        }
    }

    public void shutDown(){
        try {
            throughputLogger.dump();
        } catch (IOException e) {
            System.err.println("ERROR Failed to dump kv throughput stats to disk: " + e.getMessage());
        }

    }

}


