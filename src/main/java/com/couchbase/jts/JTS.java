package main.java.com.couchbase.jts;


import main.java.com.couchbase.jts.properties.TestProperties;
import main.java.com.couchbase.jts.worker.WorkerManager;

import java.util.Properties;
import java.io.File;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public class JTS {

    public static void main(String[] args) {
        Properties sysprops = System.getProperties();
        sysprops.setProperty("com.couchbase.forceIPv4", "false");

        TestProperties props = new TestProperties(args);
        if ((new File("logs/" + TestProperties.CONSTANT_JTS_LOG_DIR)).mkdirs()) {
            WorkerManager manager = new WorkerManager(props);
            manager.start();
        } else {
            System.err.println("Cant create logs dir " + TestProperties.CONSTANT_JTS_LOG_DIR);
            System.exit(1);
        }


    }
}
