package main.java.worker;
import main.java.drivers.Client;
/**
 * Created by oleksandr.gyryk on 11/10/17.
 */
public class DebugWorker extends Worker {

    private int debugLevel;

    public DebugWorker(int workerId, Client client, int debugLevel){
        super(workerId, client);
        this.debugLevel = debugLevel;
    }

    public void runQuery(){
        String response = clientDB.queryAndResponse();
        if ((debugLevel==0) || (!validateResponse(response))) {
            statusLogger.logMessage(response);
        }
    }


    private boolean validateResponse(String response){
        return false;
    }

}
