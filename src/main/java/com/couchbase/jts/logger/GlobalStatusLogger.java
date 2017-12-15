package main.java.com.couchbase.jts.logger;

import main.java.com.couchbase.jts.properties.TestProperties;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.IOException;


public class GlobalStatusLogger{
    private PrintWriter pw;

    public GlobalStatusLogger() {
        try {
            File file = new File("logs/" + TestProperties.CONSTANT_JTS_LOG_DIR + "/global.log");
            if (!file.exists()) { file.createNewFile(); }
            FileWriter fw = new FileWriter(file, true);
            BufferedWriter bw = new BufferedWriter(fw);
            pw = new PrintWriter(bw);
        } catch(IOException ioe){
        ioe.printStackTrace();
        }
    }

    public void logMessage(String message) {
        String fullMessage = "" + Logger.timeStampFormated() + ": " + message;
        pw.println(fullMessage);
        pw.flush();
        System.out.println(fullMessage);
    }

    public void close() {
        pw.close();
    }

}

