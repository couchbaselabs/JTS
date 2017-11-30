package main.java.logger;

import main.java.utils.LogPair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public class ThroughputLogger extends Logger{

    private long timeStart = 0;
    private int requestsSinceLastDrop = 0;
    private int aggregationBufferMS = 1000;
    private long samplesCounter = 0;

    public ThroughputLogger(int storageLimit, int loggerId){
        super(storageLimit, loggerId);
        filename = "worker_" + loggerId + "_throughput.log";
    }

    public void logRequest(){
        long logTime = Logger.timeStamp();
        if (timeStart == 0) {
            timeStart = Logger.timeStamp();
            requestsSinceLastDrop = 1;
            samplesCounter = 1;
        } else if ((logTime - timeStart) < aggregationBufferMS) {
            requestsSinceLastDrop ++;
        } else {
            timeStart = logTime;
            drop(samplesCounter, requestsSinceLastDrop);
            samplesCounter++;
            requestsSinceLastDrop = 1;
        }
    }

    public void dump() throws IOException{
        drop(samplesCounter, requestsSinceLastDrop);
        try {
            Logger.dump(filename, pool, count);
        } catch (IOException e) {
            throw e;
        }
    }


    public static float aggregate(int totalFilesExpected) throws IOException{
        List<LogPair> lines = new ArrayList<>();
        for (int i=0; i< totalFilesExpected; i++) {
            String filename = "worker_" + i + "_throughput.log";
            Stream<String> strm;
            try {
                strm = Files.lines(Paths.get(filename));
            } catch (IOException ioex) {
                System.err.println("Aggregating throughput: " + ioex.getMessage());
                continue;
            }
            try (Stream<String> stream = strm) {
                stream.forEach(x -> lines.add(new LogPair(x)));
            }
        }

        LogPair[] pairsArr = lines.toArray(new LogPair[lines.size()]);
        Arrays.sort(pairsArr, (a, b) -> a.k.compareTo(b.k));
        Logger.dump("combined_throughput.log", pairsArr, pairsArr.length);

        List<LogPair> aggregates = new ArrayList<>();
        long lastSample = -1;
        float totalrequests = 0;
        for (LogPair pair: pairsArr){
            if (lastSample < 0) {
                lastSample = pair.k;
                totalrequests += pair.v;
                continue;
            }
            if (lastSample == pair.k) {
                totalrequests += pair.v;
            } else {
                aggregates.add(new LogPair(lastSample, totalrequests));
                lastSample = pair.k;
                totalrequests = pair.v;
            }
        }

        Logger.dump("aggregated_throughput.log", aggregates.toArray(new LogPair[aggregates.size()]),
                aggregates.size());

        int totalValues = aggregates.size();
        float sum = 0;
        for (LogPair pair: aggregates){
            sum += pair.v;
        }

        if (totalValues != 0) {
            return sum/totalValues;
        }

        return 0;
    }
}
