package com.couchbase.jts.drivers;

import com.couchbase.client.core.message.query.QueryRequest;
import com.couchbase.jts.properties.TestProperties;
import com.mongodb.*;
import com.mongodb.bulk.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.QueryResult;
import org.bson.Document;

/**
 * Created by oleksandr.gyryk on 10/3/17.
 */


public class MongodbClient extends Client{

    private Random rand = new Random();
    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    private static final Integer INCLUDE = Integer.valueOf(1);
    private static MongoClient mongoClient;
    private static MongoDatabase database;
    private static ReadPreference readPreference;
    private static WriteConcern writeConcern;
    private static String collectionName;

    private QueryBlocks[] queries;
    int totalQueries = 0;

    public MongodbClient(TestProperties workload) throws Exception{
        super(workload);

        try{
            init();
            generateQueries();
        } catch (Exception ex){
            throw ex;
        }
    }

    private void generateQueries() throws IOException {
        String[][] terms = importTerms();
        int size = terms.length;
        ArrayList<QueryBlocks> queriesList = new ArrayList<>();

        for (int i = 0; i< size; i++) {
            int lineSize = terms[i].length;
            if (lineSize > 0) {
                queriesList.add(new QueryBlocks(buildTerm(terms[i]), settings));
            }
        }
        Collections.shuffle(queriesList);
        queries = queriesList.stream().toArray(QueryBlocks[]::new);
        totalQueries = queries.length;
    }

    private String buildTerm(String[] termsArray) throws IllegalArgumentException {
        String queryType = settings.get(settings.TESTSPEC_QUERY_TYPE);
        if (queryType.equals(TestProperties.CONSTANT_QUERY_TYPE_TERM)) {
            return termsArray[0] + " ";
        } else if (queryType.equals( TestProperties.CONSTANT_QUERY_TYPE_PHRASE)) {
            return "\"" + String.join(" ", termsArray) + "\"";
        } else if (queryType.equals(TestProperties.CONSTANT_QUERY_TYPE_OR)) {
            return String.join(" ", termsArray);
        }
        throw new IllegalArgumentException("Mongodb query builder: unexpected query type: " + queryType);
    }

    public float queryAndLatency() {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        QueryBlocks query = queries[rand.nextInt(totalQueries)];
        FindIterable<Document> findIterable;
        long st = System.nanoTime();
        findIterable = collection.find(query.query).projection(query.projection).sort(query.sort).limit(query.limit);
        for (Document doc : findIterable) { continue; }
        long en = System.nanoTime();
        float latency = (float) (en - st) / 1000000;
        if (findIterable.first() == null) {
            return 0;
        }

        return latency;
    }

    public void query(){
        MongoCollection<Document> collection = database.getCollection(collectionName);
        QueryBlocks query = queries[rand.nextInt(totalQueries)];
        FindIterable<Document> findIterable;
        findIterable = collection.find(query.query).projection(query.projection).sort(query.sort).limit(query.limit);
        for (Document doc : findIterable) { continue; }
    }

    public String queryDebug() {
        QueryBlocks query = queries[rand.nextInt(totalQueries)];
        MongoCollection<Document> collection = database.getCollection(collectionName);
        long st = System.nanoTime();
        FindIterable<Document> findIterable = collection.find(query.query)
                .projection(query.projection)
                .sort(query.sort)
                .limit(query.limit);
        for (Document doc : findIterable) { continue; }
        long en = System.nanoTime();
        float latency = (float) (en - st) / 1000000;
        if (findIterable.first() == null) {
            return query.searchTerm + " Not Found!";
        }
       return query.searchTerm + " : " + " (" + latency + "ms)" + findIterable.first().toString();

    }

    public void mutateRandomDoc() {
        long totalDocs = Long.parseLong(getWorkload().get(TestProperties.TESTSPEC_TOTAL_DOCS));
        long docIdLong = rand.nextLong() % totalDocs;
        String docIdHex = Long.toHexString(docIdLong);
        String originFieldName = getWorkload().get(TestProperties.TESTSPEC_QUERY_FIELD);
        String replaceFieldName = getWorkload().get(TestProperties.TESTSPEC_MUTATION_FIELD);

        MongoCollection<Document> collection = database.getCollection(collectionName);

        Document query = new Document("_id", docIdHex);

        FindIterable<Document> findIterable = collection.find(query);
        Document projection = new Document();
        projection.put(originFieldName, INCLUDE);
        projection.put(replaceFieldName, INCLUDE);
        findIterable.projection(projection);
        Document queryResult = findIterable.first();

        Document fieldsToSet = new Document();
        fieldsToSet.put(originFieldName, queryResult.getString(replaceFieldName));
        fieldsToSet.put(replaceFieldName, queryResult.getString(originFieldName));

        Document update = new Document("$set", fieldsToSet);
        UpdateResult result = collection.updateOne(query, update);
        if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
            System.err.println("Nothing updated for key " + docIdHex);
        }

    }

    public void init() throws Exception {
        INIT_COUNT.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongoClient != null) {
                return;
            }

            final String serverIP = getProp(TestProperties.MDBSPEC_SERVER);
            final String serverPort = getProp(TestProperties.MDBSPEC_PORT);
            final String databaseName = getProp(TestProperties.MDBSPEC_DATABASE);
            final String serverConnectionString = "mongodb://" + serverIP +
                    ":" + serverPort + "/" + databaseName + "?w=1";

            collectionName = getProp(TestProperties.MDBSPEC_COLLECTION);

            try {
                MongoClientURI uri = new MongoClientURI(serverConnectionString);
                readPreference = uri.getOptions().getReadPreference();
                writeConcern = uri.getOptions().getWriteConcern();
                mongoClient = new MongoClient(uri);
                database = mongoClient.getDatabase(databaseName)
                        .withReadPreference(readPreference)
                        .withWriteConcern(writeConcern);
                System.out.println("mongo client connection created with " + serverConnectionString);
            } catch (Exception e1) {
                System.err
                        .println("Could not initialize MongoDB connection pool for Loader: "
                                + e1.toString());
                e1.printStackTrace();
                throw e1;
            }
        }
    }

    private String getProp(String name) {
        return getWorkload().get(name);
    }

}

class QueryBlocks {
    public Document query;
    public Document projection;
    public Document sort;
    public int limit;
    public String searchTerm;

    public QueryBlocks(String term, TestProperties settings){
        limit = Integer.parseInt(settings.get(TestProperties.TESTSPEC_QUERY_LIMIT));
        String fieldName = "$" + settings.get(TestProperties.TESTSPEC_QUERY_FIELD);
        query =  new Document(fieldName, new Document("$search", term + "wkejfljeslfjdklsfkjdhskjfhkjdshfk"));
        projection = new Document();
        projection.put("score", new Document("$meta", "textScore"));
        projection.put("_id", 1);
        sort = new Document("score", new Document("$meta", "textScore"));
        searchTerm = term;
    }
}
