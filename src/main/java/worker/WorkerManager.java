package main.java.worker;

import main.java.drivers.Client;
import main.java.logger.LatencyLogger;
import main.java.logger.GlobalStatusLogger;
import main.java.logger.ThroughputLogger;
import main.java.properties.TestProperties;

import java.util.ArrayList;
import java.util.List;

import java.lang.reflect.*;

public class WorkerManager {

    private GlobalStatusLogger logWriter;
    private TestProperties workload;
    private List<Worker> workers;
    private List<Thread> workerThreads = new ArrayList<Thread>();

    private int aggregationStep;
    private String workerType;

    public WorkerManager(TestProperties props) {
        workload = props;
        aggregationStep = Integer.parseInt(workload.get(TestProperties.TESTSPEC_STATS_AGGR_STEP));
        workerType = workload.get(TestProperties.TESTSPEC_WORKER_TYPE);
        logWriter = new GlobalStatusLogger();
        logWriter.logMessage("Workload Manager started");
        logWriter.logMessage("Workload settings:" + '\n' + workload.getAllPropsAsString());
    }

    public void start() {
        logWriter.logMessage("Initializing workers");
        workers = initWorkers();

        logWriter.logMessage("Starting workers");
        for (Worker worker: workers) {
            workerThreads.add(new Thread(worker));
        }

        for (Thread workerThread: workerThreads) {
            workerThread.start();
        }

        for (Thread workerThread: workerThreads) {
            try {
                workerThread.join();
            } catch (InterruptedException ex) {
                logWriter.logMessage(ex.getMessage());
            }
        }
        logWriter.logMessage("All workers are done, gathering results from workers");

        try {

            if (workerType.equals("latency")) {
                float averageLatency = LatencyLogger.aggregate(workers.size(), aggregationStep);
                logWriter.logMessage("Average Latency: " + averageLatency + " ms");
                float averageThrougput = ThroughputLogger.aggregate(workers.size());
                logWriter.logMessage("Average Throughput: " + averageThrougput + " q/sec");

            } else  if (workerType.equals("throughput")) {
                float averageThrouput = ThroughputLogger.aggregate(workers.size());
                logWriter.logMessage("Average Throughput: " + averageThrouput + " q/sec");
            }

        } catch (Exception ex) {
            logWriter.logMessage(ex.getMessage());
        }
        logWriter.logMessage("All done!");
        logWriter.close();
    }

    private Client buildNewDriverObject(String className) {
        Client object = null;
        try {
            Class<?> clientClass = Class.forName(className);
            Constructor<?> ctor = clientClass.getConstructor(TestProperties.class);
            object = (Client) ctor.newInstance(workload);
        } catch (Exception ex) {
            logWriter.logMessage("Error creating DB driver: " + ex.getMessage() + "\n" + ex.getStackTrace());
            ex.printStackTrace();
            System.exit(1);
        }
        return object;
    }

    private List<Worker> initWorkers(){
        List<Worker> workersList = new ArrayList<>();
        int threads = Integer.parseInt(workload.get(TestProperties.TESTSPEC_THREADS));
        String driverClassName = workload.get(TestProperties.TESTSPEC_DRIVER);

        for (int i=0; i<threads; i++) {
            Client client = buildNewDriverObject(driverClassName);

            if (workerType.equals("debug")) {
                workersList.add(new DebugWorker(client));
            } else if (workerType.equals("latency")) {
                workersList.add(new LatencyWorker(client, i));
            } else  if (workerType.equals("throughput")) {
                workersList.add(new ThroughputWorker(client, i));
            }
        }
        return workersList;
    }
}
