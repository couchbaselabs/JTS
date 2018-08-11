package com.couchbase.jts.worker;

import com.couchbase.jts.drivers.Client;

public class ValidatedLatencyWorker extends LatencyWorker {
    public ValidatedLatencyWorker(Client client, int workerId){
        super(client, workerId);
    }

    @Override
    public void runAction(){
        float latencyMS = clientDB.queryAndLatency();
        if (latencyMS == 0) {
            latencyLogger.logLatency(latencyMS);
            throughputLogger.logRequest();
        }
    }
}
