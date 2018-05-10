package com.couchbase.jts.worker;

import com.couchbase.jts.drivers.Client;

/**
 * Created by oleksandr.gyryk on 1/18/18.
 */
public class WarmupWorker extends Worker {

    public WarmupWorker(Client client) {
        super(client);
    }

    public void runAction() {
        clientDB.query();
    }

    public void shutDown() {
        return;
    }
}
