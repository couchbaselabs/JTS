package main.java.logger;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.IOException;


public class GlobalStatusLogger{
    private PrintWriter pw;
    private String filename = "global.log";

    public GlobalStatusLogger() {
        try {
            File file = new File(filename);
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
    }

    public void close() {
        pw.close();
    }

}

