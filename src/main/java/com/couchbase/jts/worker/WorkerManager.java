package main.java.com.couchbase.jts.worker;

import main.java.com.couchbase.jts.drivers.Client;
import main.java.com.couchbase.jts.logger.LatencyLogger;
import main.java.com.couchbase.jts.logger.GlobalStatusLogger;
import main.java.com.couchbase.jts.logger.ThroughputLogger;
import main.java.com.couchbase.jts.properties.TestProperties;

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
            logWriter.logMessage("Error creating DB driver: " + ex.getMessage() + "\n" + ex.getStackTrace().toString());
            ex.printStackTrace();
            System.exit(1);
        }
        return object;
    }

    private List<Worker> initWorkers(){
        int query_workers = Integer.parseInt(workload.get(TestProperties.TESTSPEC_QUERY_WORKERS));
        if (query_workers < 1) {
            logWriter.logMessage("No workers to execute. Exiting...")
            System.exit(0);
        }

        List<Worker> workersList = new ArrayList<>();
        String driverClassName = workload.get(TestProperties.TESTSPEC_DRIVER);

        for (int i=0; i<query_workers; i++) {
            Client client = buildNewDriverObject(driverClassName);
            if (workerType.equals("debug")) {
                workersList.add(new DebugWorker(client));
            } else if (workerType.equals("latency")) {
                workersList.add(new LatencyWorker(client, i));
            } else  if (workerType.equals("throughput")) {
                workersList.add(new ThroughputWorker(client, i));
            } else if (workerType.equals("warmup")) {
                workersList.add(new WarmupWorker(client));
            }
        }

        int kv_workers = Integer.parseInt(workload.get(TestProperties.TESTSPEC_KV_WORKERS));
        int kv_throughput_goal = Integer.parseInt(workload.get(TestProperties.TESTSPEC_KV_THROUGHPUT_GOAL));

        for (int i=0; i<kv_workers; i++) {
            Client client = buildNewDriverObject(driverClassName);
            workersList.add(new KVWorker(client, i, kv_workers, kv_throughput_goal));
        }

        return workersList;
    }
}
