package logger;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.IOException;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public class LatencyLogger extends Logger{

    private String filename;

    public LatencyLogger(int storageLimit, int loggerId){
        super(storageLimit, loggerId);
        filename = "worker_" + loggerId + "_latency.log";
    }

    public void logLatency(float latency){
        drop(latency);
    }

    public void dump() throws IOException{
        try {
            dump(filename, pool);
        } catch (IOException e) {
            throw e;
        }
    }

}
