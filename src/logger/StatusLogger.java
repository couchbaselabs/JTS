package logger;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.IOException;


public class StatusLogger extends Logger{
    private PrintWriter pw;
    private boolean printMessage = false;
    private String filename;

    public StatusLogger(String fileName, boolean isDebugMode) {
        super(fileName);
        printMessage = isDebugMode;
        this.filename = fileName;

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
        String fullMessage = "" + timeStampFormated() + ": " + message;
        pw.println(fullMessage);
        pw.flush();
        if (printMessage) { System.out.println(fullMessage); }
    }


    public void close() {
        pw.close();
    }

}

