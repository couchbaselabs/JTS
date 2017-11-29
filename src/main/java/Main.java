package main.java;


import main.java.properties.TestProperties;
import main.java.worker.WorkerManager;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public class Main {

    public static void main(String[] args) {
        TestProperties props = new TestProperties(args);
        WorkerManager manager = new WorkerManager(props);
        manager.start();
    }
}
