package worker;

import drivers.Client;

/**
 * Created by oleksandr.gyryk on 11/10/17.
 */
public class DefaultWorker extends Worker {

    public DefaultWorker(int workerId, Client client){
        super(workerId, client);
    }

    public void runQuery(){
        latencyLogger.logLatency(clientDB.queryAndLatency());
    }


}
