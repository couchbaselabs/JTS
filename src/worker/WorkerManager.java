package worker;

import drivers.Client;
import logger.LatencyLogger;
import logger.StatusLogger;
import properties.TestProperties;

import javax.imageio.IIOException;
import java.util.ArrayList;
import java.util.List;

import java.lang.reflect.*;

public class WorkerManager {

    private StatusLogger logWriter;
    private TestProperties workload;
    private List<Worker> workers;
    private List<Thread> workerThreads = new ArrayList<Thread>();

    public WorkerManager(TestProperties props) {
        workload = props;
        logWriter = new StatusLogger("global.log", workload.isDebugMode());

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
            LatencyLogger.aggregate(workers.size());
        } catch (Exception ex) {
            logWriter.logMessage(ex.getMessage());
        }
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
            workersList.add(new DefaultWorker(i, client));
        }
        return workersList;
    }




}
