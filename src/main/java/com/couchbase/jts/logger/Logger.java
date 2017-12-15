package main.java.com.couchbase.jts.logger;

import main.java.com.couchbase.jts.properties.TestProperties;
import main.java.com.couchbase.jts.utils.LogPair;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Random;
import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public class Logger {

    protected LogPair[] pool;
    protected int count = 0;
    protected boolean overflow = false;
    protected int limit;
    protected final Random RAND = new Random();

    public Logger(int storageLimit, int workerId) {
        limit = storageLimit;
        pool = new LogPair[limit];
    }


    protected void drop(long time, float value){
        int id = 0;
        if (overflow) {
            id = RAND.nextInt(limit);
        } else {
            id = count;
            count++;
            if (count >= limit) {
                overflow = true;
            }
        }
        pool[id] = new LogPair(time, value);
    }


    protected void dump(String filename) throws IOException{
        try {
            Logger.dump(filename, pool, count);
        } catch (IOException e) {
            throw e;
        }
    }

    public static void dump(String filename, LogPair[] customPool, int count) throws IOException {
        LogPair[] nonEmpty = new LogPair[count];
        filename = "logs/" + TestProperties.CONSTANT_JTS_LOG_DIR + "/" + filename;
        for (int i = 0; i< count; i++) {
            nonEmpty[i] = customPool[i];
        }

        Arrays.sort(nonEmpty, (a, b) -> a.k.compareTo(b.k));
        try {
            PrintWriter dumpFile = new PrintWriter(filename, "UTF-8");
            for (LogPair pair:nonEmpty){
                dumpFile.println("" + pair.k + ":" + pair.v);
            }
            dumpFile.close();
        } catch (IOException e) {
            throw e;
        }
    }

    protected static long timeStamp() {
        return System.currentTimeMillis();
    }

    protected static String timeStampFormated() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        Date resultdate = new Date(System.currentTimeMillis());
        return sdf.format(resultdate);
    }


}
