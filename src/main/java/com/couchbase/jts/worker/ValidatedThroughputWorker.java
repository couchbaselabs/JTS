package com.couchbase.jts.worker;

import com.couchbase.jts.drivers.Client;

public class ValidatedThroughputWorker extends ThroughputWorker {

    public ValidatedThroughputWorker(Client client, int workerId){
        super(client, workerId);
    }

    @Override
    public void runAction(){
        if (clientDB.queryAndSuccess()){
            throughputLogger.logRequest();
        }

    }

}
