package com.couchbase.jts.worker;

import com.couchbase.jts.drivers.Client;
import com.couchbase.jts.logger.ThroughputLogger;
import com.couchbase.jts.properties.TestProperties;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import com.couchbase.jts.logger.GlobalStatusLogger;

/**
 * Created by oleksandr.gyryk on 12/7/17.
 */
public class KVWorker extends Worker {

    private int totalWorkers;
    private int throughputGoal;
    private GlobalStatusLogger logWriter = new GlobalStatusLogger();

    public KVWorker(Client client, int workerId, int totalWorkers, int throughputGoal) {
        super(client);
        this.totalWorkers = totalWorkers;
        this.throughputGoal = throughputGoal;
    }

    public void runAction(){
        try {
            if (!TestProperties.TESTSPEC_MUTATION_FIELD.equals("test_mutation_field") && !TestProperties.TESTSPEC_MUTATION_FIELD.equals("false") ){
                long st = System.nanoTime();
                clientDB.mutateRandomDoc();
                long en = System.nanoTime();
                long latency =  (en - st) / 1000000;
                if (throughputGoal >= 0) {
                    float expectedDelayMC = (totalWorkers / (float) throughputGoal) * 1000;
                    if (expectedDelayMC > latency) {
                        long delayMS = (long) expectedDelayMC - latency;
                        TimeUnit.MILLISECONDS.sleep(delayMS);
                    }
                }
            }

        } catch (Exception ex) {
            return;
        }
    }

    public void shutDown(){
        return;
    }

}

