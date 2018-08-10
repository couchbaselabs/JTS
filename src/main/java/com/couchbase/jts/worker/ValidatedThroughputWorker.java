package com.couchbase.jts.worker;

import com.couchbase.jts.drivers.Client;
import com.couchbase.jts.logger.ThroughputLogger;

public class ValidatedThroughputWorker extends ThroughputWorker {

    public ValidatedThroughputWorker(Client client, int workerId){
        super(client, workerId);
    }

    @Override
    public void runAction(){
        if (clientDB.querySuccessOnly()){
            throughputLogger.logRequest();
        }

    }

}
