package drivers;

import properties.TestProperties;

import java.util.List;

/**
 * Created by oleksandr.gyryk on 10/3/17.
 */
abstract public class Client {

    protected TestProperties settings;


    public Client(TestProperties workload) {
        settings = workload;
    }


    public TestProperties getWorkload() {
        return settings;
    }

    abstract public boolean query();
    abstract public String queryAndResponse();
}
