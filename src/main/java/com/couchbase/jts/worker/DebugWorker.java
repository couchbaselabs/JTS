package main.java.com.couchbase.jts.worker;
import main.java.com.couchbase.jts.drivers.Client;

/**
 * Created by oleksandr.gyryk on 11/10/17.
 */
public class DebugWorker extends Worker {

    public DebugWorker(Client client){
        super(client);
    }

    public void runAction(){
        System.out.println(clientDB.queryDebug());
    }

    public void shutDown() {
    }

}
