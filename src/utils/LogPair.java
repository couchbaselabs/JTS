package utils;

/**
 * Created by oleksandr.gyryk on 10/2/17.
 */
public class LogPair {
    public Long k;
    public String v;

    public LogPair(long timeStamp, String message) {
        k = timeStamp;
        v = message;
    }
}
