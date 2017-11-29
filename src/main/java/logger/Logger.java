package main.java.logger;

import main.java.utils.LogPair;

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
    protected int counter = 0;
    protected boolean overflow = false;
    protected int limit;
    protected final Random RAND = new Random();
    protected int id;
    protected String filename;

    public Logger(String fileName) {
        filename = fileName;
    }

    public Logger(int storageLimit, int loggerId) {
        limit = storageLimit;
        pool = new LogPair[limit];
        id = loggerId;
    }

    protected void drop(float value){
        int id = 0;
        if (overflow) {
            id = RAND.nextInt(limit);
        } else {
            id = counter;
            counter ++;
            if (counter >= limit) {
                overflow = true;
            }
        }
        pool[id] = new LogPair(timeStamp(), value);
    }

    public static void dump(String filename, LogPair[] customPool, int count) throws IOException {
        LogPair[] nonEmpty = new LogPair[count];

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

    private long timeStamp() {
        return System.currentTimeMillis();
    }

    protected String timeStampFormated() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        Date resultdate = new Date(System.currentTimeMillis());
        return sdf.format(resultdate);
    }


}
