package main.java.drivers;

import main.java.properties.TestProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.*;
import java.nio.file.*;
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

    protected String[][] importTerms() throws IOException{
        List<String[]> lines = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(settings.get(TestProperties.TESTSPEC_TESTDATA_FILE)))) {
            stream.forEach(x -> lines.add(x.split(" ")));
        }
        Collections.shuffle(lines);
        String[][] response = lines.stream().toArray(String[][]::new);
        return response;
    }

    abstract public float queryAndLatency();
    abstract public String queryDebug();
    abstract public void query();
    abstract public void mutateRandomDoc();
}
