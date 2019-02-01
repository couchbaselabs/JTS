package com.couchbase.jts.utils;

import com.couchbase.jts.properties.TestProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;
import java.util.Random;

public class DataGen {

    /*
        a	two_by_three 16
        b	three_by_four 23
        c	two_by_four 25
        d	one_by_five_keyword 14
        e	five_by_six 13
        f	ten_by_seven 11
        g	sixteen_by_eight 8
        h	boolean1
        k	boolean2
        l	number
        m	date
        n	lat
        o	long
        p	wiki
        r	type
        s	id
     */


    private TestProperties settings;
    private HashMap<Integer, char[]> charsetsMap = new HashMap<>();
    private static final int ITEMS = 1000;
    private Random rand = new Random();


    public DataGen(TestProperties settings) {
        this.settings = settings;
        charsetsMap.put(8,  "zxcvbnma".toCharArray());
        charsetsMap.put(11, "qwertyuiopa".toCharArray());
        charsetsMap.put(13, "asdfghjkloiuy".toCharArray());
        charsetsMap.put(14, "qazxswedcvfrtg".toCharArray());
        charsetsMap.put(16, "zxcvbnmjhgfdsaqw".toCharArray());
        charsetsMap.put(23, "zaqwsxcderfvbgtyhnmjuik".toCharArray());
        charsetsMap.put(25, "qwertyuiopasdfghjklzxcvbn".toCharArray());

    }


    public HashMap<String, ArrayList<String>> initValues(HashMap<String, String> fieldsMap) throws IOException{
        if (settings.get(TestProperties.TESTSPEC_DATA_DATAGEN).equals("file")) {
            return importValues(fieldsMap);
        }

        return generateValues(fieldsMap);

    }

    private HashMap<String, ArrayList<String>> generateValues(HashMap<String, String> fieldsMap) throws IOException {
        HashMap<String, ArrayList<String>> valuesMap = new HashMap<>();
        valuesMap.put("a", getSequences(3,16));
        valuesMap.put("b", getSequences(4,23));
        valuesMap.put("c", getSequences(4,25));
        valuesMap.put("d", getSequences(5,14));
        valuesMap.put("e", getSequences(6,13));
        valuesMap.put("f", getSequences(7,11));
        valuesMap.put("g", getSequences(8,8));
        valuesMap.put("h", getBools());
        valuesMap.put("k", getBools());
        valuesMap.put("l", getNumbers());
        valuesMap.put("m", getDates());
        valuesMap.put("n", getLats());
        valuesMap.put("o", getLongs());
        valuesMap.put("r", getTypes());
        valuesMap.put("s", getIds());


        return valuesMap;
    }

    private ArrayList<String> getSequences(int len, int symbols){
        char[] setMap = charsetsMap.get(symbols);
        ArrayList<String> result = new ArrayList<>();
        for (int i=0; i < ITEMS; i++){
            StringBuffer sequenceBuilder = new StringBuffer();
            for (int j=0; j < len; j++) {
                int idx = rand.nextInt(symbols);
                sequenceBuilder.append(setMap[idx]);
            }
            result.add(sequenceBuilder.toString());
        }
        return result;
    }

    private ArrayList<String> getBools(){
        ArrayList<String> result = new ArrayList<>();
        for (int i=0; i < ITEMS; i++){
            if (rand.nextInt(100) > 50){
                result.add("true");
            } else {
                result.add("false");
            }
        }
        return result;
    }

    private ArrayList<String> getDates(){
        ArrayList<String> result = new ArrayList<>();
        StringBuffer dateBuffer = new StringBuffer();

        for (int i=0; i< ITEMS; i++) {
            dateBuffer.append("2018-");
            int m = 1 + rand.nextInt(10);
            if (m < 10){
                dateBuffer.append("0");
            }
            dateBuffer.append(String.valueOf(m));
            dateBuffer.append("-");
            dateBuffer.append(String.valueOf(10 + rand.nextInt(11)));
        }
        return result;
    }

    private ArrayList<String> getNumbers(){
        ArrayList<String> result = new ArrayList<>();
        for (int i=0; i< ITEMS; i++) {
            result.add(String.valueOf(rand.nextInt(1000000000)*10));
        }
        return result;
    }

    private ArrayList<String> getLats(){
        ArrayList<String> result = new ArrayList<>();
        for (int i=0; i< ITEMS; i++) {
            result.add(String.valueOf((rand.nextInt(180000000) - 90000000)/1000000));
        }
        return result;
    }

    private ArrayList<String> getLongs(){
        ArrayList<String> result = new ArrayList<>();
        for (int i=0; i< ITEMS; i++) {
            result.add(String.valueOf((rand.nextInt(380000000) - 180000000)/1000000));
        }
        return result;
    }


    private ArrayList<String> getIds(){
        ArrayList<String> result = new ArrayList<>();
        StringBuffer dateBuffer = new StringBuffer();

        for (int i=0; i< ITEMS; i++) {
            dateBuffer.append("2018-");
            int m = 1 + rand.nextInt(10);
            if (m < 10){
                dateBuffer.append("0");
            }
            dateBuffer.append(String.valueOf(m));
            dateBuffer.append("-");
            dateBuffer.append(String.valueOf(10 + rand.nextInt(11)));
        }
        return result;
    }


    private ArrayList<String> getTypes(){
        return new ArrayList<>();
    }


    private HashMap<String, ArrayList<String>> importValues(HashMap<String, String> fieldsMap) throws IOException {
        HashMap<String, ArrayList<String>> valuesMap = new HashMap<>();
        Set<String> keys = fieldsMap.keySet();
        ArrayList<String> keysList = new ArrayList<>();
        for (String k:keys ) {
            keysList.add(k);
        }
        Collections.sort(keysList);

        String[][] lines = importTerms();
        if (lines.length == 0 || lines[0].length > keysList.size()) {
            throw new IOException();
        }

        for (String[] terms : lines) {
            for (int i = 0; i< terms.length; i++) {
                String key = keysList.get(i);
                if (valuesMap.containsKey(key)){
                    ArrayList<String>valuesList = valuesMap.get(key);
                    valuesList.add(terms[i]);
                } else {
                    ArrayList<String>valuesList = new ArrayList<>();
                    valuesList.add(terms[i]);
                    valuesMap.put(key, valuesList);
                }
            }
        }

        return valuesMap;

    }


    private String[][] importTerms() throws IOException{
        List<String[]> lines = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(settings.get(TestProperties.TESTSPEC_TESTDATA_FILE)))) {
            stream.forEach(x -> lines.add(x.split(" ")));
        }
        Collections.shuffle(lines);
        String[][] response = lines.stream().toArray(String[][]::new);
        return response;
    }


}
