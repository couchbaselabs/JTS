package drivers;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.search.queries.AbstractFtsQuery;
import properties.TestProperties;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by oleksandr.gyryk on 10/3/17.
 */


public class CouchbaseClient extends Client{

    private Bucket bucket;
    private List<AbstractFtsQuery> queryList = new LinkedList<>();

    public CouchbaseClient(TestProperties workload) {
        super(workload);
        connect();
        generateQueries();
    }


    private void connect() {

    }


    private void generateQueries() {

    }


   public boolean query() {
        return true;
   }

   public String queryAndResponse(){
        return "";
   }

}
