package logger;

import utils.LogPair;

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

    protected void drop(String message){
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
        pool[id] = new LogPair(timeStamp(),message);
    }

    public void dump(String filename, LogPair[] customPool) throws IOException {
        Arrays.sort(customPool, (a, b) -> a.k.compareTo(b.k));
        try {
            PrintWriter dumpFile = new PrintWriter(id + filename + ".log", "UTF-8");
            for (LogPair pair:customPool){
                dumpFile.println("" + pair.k + ":" + pair.v);
            }
            dumpFile.close();
        } catch (IOException e) {
            throw e;
        }
    }

    protected long timeStamp() {
        return System.currentTimeMillis();
    }

    protected String timeStampFormated() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        Date resultdate = new Date(System.currentTimeMillis());
        return sdf.format(resultdate);
    }
}
