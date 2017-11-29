package main.java.utils;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public class LogPair {
    public Long k;
    public Float v;

    public LogPair(long timeStamp, float value) {
        k = timeStamp;
        v = value;
    }

    public LogPair(String str){
        String[] pairStr = str.split(":");
        k = Long.parseLong(pairStr[0]);
        v = (pairStr.length > 1) ? Float.parseFloat(pairStr[1]) : 0;
    }
}
