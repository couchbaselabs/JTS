package main.java.worker;
import main.java.drivers.Client;

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
