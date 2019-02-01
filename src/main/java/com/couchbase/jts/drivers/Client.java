package com.couchbase.jts.drivers;

import com.couchbase.jts.properties.TestProperties;
import com.couchbase.jts.utils.DataGen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.*;
import java.nio.file.*;
import java.util.List;

/**
 * Created by oleksandr.gyryk on 10/3/17.
 */
abstract public class Client {

    protected TestProperties settings;
    protected HashMap<String, String> fieldsMap = new HashMap<>();
    protected HashMap<String, ArrayList<String>> valuesMap = new HashMap<>();

    public Client(TestProperties workload) throws IOException {
        settings = workload;
        importFields();
        importValues();
    }

    public TestProperties getWorkload() {
        return settings;
    }


    private void importFields(){
        String[] tokens = settings.get(TestProperties.TESTSPEC_QUERY_FIELD_MAP).split("\\|");
        for (int i=0; i < tokens.length-1; i+=2){
            fieldsMap.put(tokens[i], tokens[i+1]);
        }
    }

    private void importValues() throws IOException{
        DataGen gen = new DataGen(settings);
        valuesMap = gen.initValues(fieldsMap);
    }



    abstract public float queryAndLatency();
    abstract public String queryDebug();
    abstract public void query();
    abstract public Boolean queryAndSuccess();
    abstract public void kv();


}
