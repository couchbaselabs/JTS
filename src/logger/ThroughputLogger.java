package logger;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public class ThroughputLogger extends Logger{

    private String filename;
    private int step;

    public ThroughputLogger(int storageLimit, int loggerId, int aggregationStep){
        super(storageLimit, loggerId);
        step = aggregationStep;
        filename = "worker_" + loggerId + "_throughput.log";
    }

    public void logTotalRequests(String message){
        drop(message);
    }

    public void dump() throws IOException {
        int poolLength = pool.length;
        if (poolLength == 0) {return;}
        Arrays.sort(pool, (a, b) -> a.k.compareTo(b.k));
        List<LogPair> averages = new ArrayList<>();
        long startedAt = pool[0].k;
        float startedWith = Float.parseFloat(pool[0].v);
        int samplesCounter = 0;
        for  (int i = 1; i < poolLength; i++) {
            samplesCounter ++;
            while ((i< poolLength) && (pool[i].k < (startedAt + step ))) {
                i++;
            }
            float finishedWith = Float.parseFloat(pool[i-1].v);
            float diff = finishedWith - startedWith;
            if (diff < 0) {
                diff = 0;
            }
            float avgThroughput = 1000 * (diff / step);
            averages.add(new LogPair(samplesCounter * step / 1000, Float.toString(avgThroughput)));
            startedWith = Float.parseFloat(pool[i-1].v);
        }

        try {
            LogPair[] averagesArr = new LogPair[averages.size()];
            dump(filename, averages.toArray(averagesArr));
        } catch (IOException e) {
            throw e;
        }

    }
}
