package utils;

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
}
