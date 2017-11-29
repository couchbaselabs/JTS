package logger;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import utils.LogPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.*;
import java.nio.file.*;
import java.util.List;

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
            Logger.dump(filename, pool, counter);
        } catch (IOException e) {
            throw e;
        }
    }

    public static void aggregate(int totalFilesExpected) throws IOException{
        List<LogPair> lines = new ArrayList<>();
        for (int i=0; i< totalFilesExpected; i++) {
            String filename = "worker_" + i + "_latency.log";
            Stream<String> strm;
            try {
                strm = Files.lines(Paths.get(filename));
            } catch (IOException ioex) {
                System.err.println("Aggregating latency: " + ioex.getMessage());
                continue;
            }
            try (Stream<String> stream = strm) {
                stream.forEach(x -> lines.add(new LogPair(x)));
            }
        }

        LogPair[] pairsArr = lines.toArray(new LogPair[lines.size()]);
        Arrays.sort(pairsArr, (a, b) -> a.k.compareTo(b.k));
        Logger.dump("combined_latency.log", pairsArr, pairsArr.length);

        List<LogPair> aggregates = new ArrayList<>();
        int aggCounter = 0;
        int stepCounter = 0;
        float aggStorage = 0;
        long lastTimeStamp = 0;
        int aggregationStep = 1000;

        for (LogPair pair: pairsArr){
            if (lastTimeStamp + aggregationStep > pair.k) {
                aggCounter++;
                aggStorage += pair.v;
            } else {
                stepCounter++;
                lastTimeStamp = pair.k;
                aggStorage = pair.v;
                aggCounter = 1;
                float avg = (aggCounter == 0)? 0: (float) aggStorage / aggCounter;
                aggregates.add(new LogPair(stepCounter, avg));
            }
        }
        Logger.dump("aggregated_latency.log", aggregates.toArray(new LogPair[aggregates.size()]),
                aggregates.size());
    }

}


