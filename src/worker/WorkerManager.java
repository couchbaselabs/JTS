package worker;

import drivers.Client;
import logger.StatusLogger;
import properties.TestProperties;

import java.util.ArrayList;
import java.util.List;

import java.lang.reflect.*;

public class WorkerManager {

    private StatusLogger logWriter;
    private TestProperties workload;
    private List<Worker> workers;

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
            new Thread(worker).start();
        }
        // wait untill all finished
        // call ShutDown method for all workers
        //gather logs
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
            workersList.add(new Worker(i, client));
        }
        return workersList;
    }




}
