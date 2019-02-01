package com.couchbase.jts.drivers;

import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.queries.*;
import com.couchbase.jts.properties.TestProperties;

import java.util.*;

public class CouchbaseQueryBuilder {


    protected TestProperties settings;
    protected HashMap<String, String> fieldsMap = new HashMap<>();
    protected HashMap<String, ArrayList<String>> valuesMap = new HashMap<>();
    private int ITEMS = 10000;
    private Random rand = new Random();



    public CouchbaseQueryBuilder(HashMap<String, String> fieldsMap, HashMap<String, ArrayList<String>> valuesMap, TestProperties settings) {

        this.settings = settings;
        this.fieldsMap = fieldsMap;
        this.valuesMap = valuesMap;

    }


    public List<SearchQuery> buildTermQueries(int limit, String indexName) {
        String fieldName = fieldsMap.get("a");
        int filedValuesTotal = valuesMap.get("a").size();
        List<SearchQuery>  queryList = new ArrayList<>();

        for (int i=0; i<ITEMS; i++){
            queryList.add(new SearchQuery(indexName, SearchQuery.term(valuesMap.get("a")
                    .get(rand.nextInt(filedValuesTotal))).field(fieldName))
                    .limit(limit));
        }
        return queryList;
    }


    public List<SearchQuery> buildAndQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldA = fieldsMap.get("a");
        String fieldB = fieldsMap.get("b");
        int valuesTotalA = valuesMap.get("a").size();
        int valuesTotalB = valuesMap.get("b").size();

        for (int i=0; i < ITEMS; i++){
            String valA = valuesMap.get("a").get(rand.nextInt(valuesTotalA));
            String valB = valuesMap.get("b").get(rand.nextInt(valuesTotalB));
            TermQuery lt = SearchQuery.term(valA).field(fieldA);
            TermQuery rt = SearchQuery.term(valB).field(fieldB);
            ConjunctionQuery conjSQ = SearchQuery.conjuncts(lt, rt);
            queryList.add(new SearchQuery(indexName, conjSQ).limit(limit));
        }
        return queryList;
    }


    public List<SearchQuery>  buildOrQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldA = fieldsMap.get("a");
        String fieldB = fieldsMap.get("b");
        int valuesTotalA = valuesMap.get("a").size();
        int valuesTotalB = valuesMap.get("b").size();

        for (int i=0; i < ITEMS; i++){
            String valA = valuesMap.get("a").get(rand.nextInt(valuesTotalA));
            String valB = valuesMap.get("b").get(rand.nextInt(valuesTotalB));
            TermQuery lt = SearchQuery.term(valA).field(fieldA);
            TermQuery rt = SearchQuery.term(valB).field(fieldB);
            DisjunctionQuery disSQ = SearchQuery.disjuncts(lt, rt);
            queryList.add(new SearchQuery(indexName, disSQ).limit(limit));
        }
        return queryList;

    }


    public List<SearchQuery> buildAndOrOrQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldA = fieldsMap.get("a");
        String fieldB = fieldsMap.get("b");
        String fieldC = fieldsMap.get("c");
        int lenA = valuesMap.get("a").size();
        int lenB = valuesMap.get("b").size();
        int lenC = valuesMap.get("c").size();

        for (int i = 0; i< ITEMS; i++) {
            String valA = valuesMap.get("a").get(rand.nextInt(lenA));
            String valB = valuesMap.get("b").get(rand.nextInt(lenB));
            String valC = valuesMap.get("c").get(rand.nextInt(lenC));
            TermQuery lt = SearchQuery.term(valA).field(fieldA);
            TermQuery mt = SearchQuery.term(valB).field(fieldB);
            TermQuery rt = SearchQuery.term(valC).field(fieldC);

            DisjunctionQuery disSQ = SearchQuery.disjuncts(mt, rt);
            ConjunctionQuery conjSQ = SearchQuery.conjuncts(disSQ, lt);

            queryList.add(new SearchQuery(indexName, conjSQ).limit(limit));
        }

        return queryList;
    }



    public List<SearchQuery> buildFuzzyQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldA = fieldsMap.get("a");
        int valuesTotalA = valuesMap.get("a").size();
        int valuesTotalB = valuesMap.get("b").size();

        for (int i=0; i < ITEMS; i++){
            String valA = valuesMap.get("a").get(rand.nextInt(valuesTotalA));
            String valB = valuesMap.get("b").get(rand.nextInt(valuesTotalB));
            int fuzziness = Integer.parseInt(valB);
            queryList.add(new SearchQuery(indexName, SearchQuery.term(valA).field(fieldA).
                    fuzziness(fuzziness)).limit(limit));
        }
        return queryList;
    }


    public List<SearchQuery> buildPhraseQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldA = fieldsMap.get("a");
        int valuesTotalA = valuesMap.get("a").size();
        int valuesTotalB = valuesMap.get("b").size();

        for (int i=0; i < ITEMS; i++){
            String valA = valuesMap.get("a").get(rand.nextInt(valuesTotalA));
            String valB = valuesMap.get("b").get(rand.nextInt(valuesTotalB));

            MatchPhraseQuery mphSQ = SearchQuery.matchPhrase(valA + " " + valB).field(fieldA);
            queryList.add(new SearchQuery(indexName, mphSQ).limit(limit));
        }
        return queryList;
    }


    public List<SearchQuery> buildPrefixQueries(int limit, String indexName) {
        String fieldName = fieldsMap.get("a");
        int valuesTotal = valuesMap.get("a").size();
        List<SearchQuery>  queryList = new ArrayList<>();

        for (int i=0; i<ITEMS; i++){
            PrefixQuery prefSQ = SearchQuery.prefix(valuesMap.get("a")
                    .get(rand.nextInt(valuesTotal))).field(fieldName);
            queryList.add(new SearchQuery(indexName, prefSQ).limit(limit));
        }
        return queryList;
    }

    public List<SearchQuery> buildWildcardQueries(int limit, String indexName) {
        String fieldName = fieldsMap.get("a");
        int valuesTotal = valuesMap.get("a").size();
        List<SearchQuery>  queryList = new ArrayList<>();

        for (int i=0; i<ITEMS; i++){
            WildcardQuery wcSQ = SearchQuery.wildcard(valuesMap.get("a")
                    .get(rand.nextInt(valuesTotal))).field(fieldName);
            queryList.add(new SearchQuery(indexName, wcSQ).limit(limit));
        }
        return queryList;
    }

    public List<SearchQuery> buildFacetQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldA = fieldsMap.get("a");
        int valuesTotalA = valuesMap.get("a").size();
        int valuesTotalB = valuesMap.get("b").size();

        for (int i=0; i<ITEMS; i++){
            TermQuery tSQ = SearchQuery.term(valuesMap.get("a")
                    .get(rand.nextInt(valuesTotalA))).field(fieldA);
            String[] dates = valuesMap.get("b")
                    .get(rand.nextInt(valuesTotalB)).split(":");
            SearchQuery resultQuery = new SearchQuery(indexName, tSQ)
                    .addFacet("bydaterange", SearchFacet.date("date", limit)
                            .addRange("dateranges", dates[0], dates[1]))
                    .limit(limit);
            queryList.add(resultQuery);
        }
        return queryList;
    }


    public List<SearchQuery> buildNumericQueries(int limit, String indexName) {
        String fieldName = fieldsMap.get("a");
        int valuesTotal = valuesMap.get("a").size();
        List<SearchQuery>  queryList = new ArrayList<>();
        for (int i=0; i<ITEMS; i++){
            String[] minmax = valuesMap.get("a")
                    .get(rand.nextInt(valuesTotal)).split(":");
            NumericRangeQuery nrgSQ = SearchQuery.numericRange()
                    .max(Double.parseDouble(minmax[0]), true)
                    .min(Double.parseDouble(minmax[1]), true).field(fieldName);
            queryList.add(new SearchQuery(indexName, nrgSQ).limit(limit));
        }
        return queryList;
    }



    public List<SearchQuery> build1BTermQueries(int limit, String indexName) {
        String fieldName = fieldsMap.get("e");
        int filedValuesTotal = valuesMap.get("e").size();
        List<SearchQuery>  queryList = new ArrayList<>();
        for (int i=0; i<ITEMS; i++){
            String term = valuesMap.get("e").get(rand.nextInt(filedValuesTotal));
            SearchQuery q = new SearchQuery(indexName, SearchQuery.term(term).field(fieldName))
                    .limit(limit);
            queryList.add(q);
        }
        return queryList;
    }

    public List<SearchQuery> build1BTermArrayQueries(int limit, String indexName) {
        String fieldName = fieldsMap.get("g");
        int filedValuesTotal = valuesMap.get("g").size();
        List<SearchQuery>  queryList = new ArrayList<>();
        for (int i=0; i<ITEMS; i++){
            queryList.add(new SearchQuery(indexName, SearchQuery.term(valuesMap.get("g")
                    .get(rand.nextInt(filedValuesTotal))).field(fieldName))
                    .limit(limit));
        }
        return queryList;
    }

    public List<SearchQuery> build1BAndQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldF = fieldsMap.get("f");
        int valuesTotalF = valuesMap.get("f").size();

        for (int i=0; i < ITEMS; i++){
            String valLT = valuesMap.get("f").get(rand.nextInt(valuesTotalF));
            String valRT = valuesMap.get("f").get(rand.nextInt(valuesTotalF));
            TermQuery lt = SearchQuery.term(valLT).field(fieldF);
            TermQuery rt = SearchQuery.term(valRT).field(fieldF);
            ConjunctionQuery conjSQ = SearchQuery.conjuncts(lt, rt);
            queryList.add(new SearchQuery(indexName, conjSQ).limit(limit));
        }
        return queryList;
    }


    public List<SearchQuery>  build1BOrQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldF = fieldsMap.get("f");
        int valuesTotalF = valuesMap.get("f").size();

        for (int i=0; i < ITEMS; i++){
            String valLT = valuesMap.get("f").get(rand.nextInt(valuesTotalF));
            String valRT = valuesMap.get("f").get(rand.nextInt(valuesTotalF));
            TermQuery lt = SearchQuery.term(valLT).field(fieldF);
            TermQuery rt = SearchQuery.term(valRT).field(fieldF);
            DisjunctionQuery disSQ = SearchQuery.disjuncts(lt, rt);
            queryList.add(new SearchQuery(indexName, disSQ).limit(limit));
        }
        return queryList;
    }

    /*
    public List<SearchQuery> build1BDCDCDQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        BooleanFieldQuery bqt =  SearchQuery.booleanField(true).field("b_1");
        BooleanFieldQuery bqf =  SearchQuery.booleanField(false).field("b_1");
        queryList.add(new SearchQuery(indexName, bqt));
        queryList.add(new SearchQuery(indexName, bqf));
        //bucket.query(queries[rand.nextInt(totalQueries)]);

        return queryList;
    }
    */

    public List<SearchQuery> build1BDCDCDQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldA = fieldsMap.get("a");
        String fieldB = fieldsMap.get("b");
        String fieldC = fieldsMap.get("c");
        String fieldH = fieldsMap.get("h");
        String fieldK = fieldsMap.get("k");
        int lenA = valuesMap.get("a").size();
        int lenB = valuesMap.get("b").size();
        int lenC = valuesMap.get("c").size();
        int lenH = valuesMap.get("h").size();
        int lenK = valuesMap.get("k").size();

        for (int i = 0; i< ITEMS; i++) {
            String valA = valuesMap.get("a").get(rand.nextInt(lenA));
            String valB1 = valuesMap.get("b").get(rand.nextInt(lenB));
            String valB2 = valuesMap.get("b").get(rand.nextInt(lenB));
            String valC = valuesMap.get("c").get(rand.nextInt(lenC));
            String valH = valuesMap.get("h").get(rand.nextInt(lenH));
            String valK = valuesMap.get("k").get(rand.nextInt(lenK));

            TermQuery at = SearchQuery.term(valA).field(fieldA);
            TermQuery b1t = SearchQuery.term(valB1).field(fieldB);
            TermQuery b2t = SearchQuery.term(valB2).field(fieldB);
            TermQuery ct = SearchQuery.term(valC).field(fieldC);

            BooleanFieldQuery ht = SearchQuery.booleanField(Boolean.parseBoolean(valH)).field(fieldH);
            BooleanFieldQuery kt = SearchQuery.booleanField(Boolean.parseBoolean(valK)).field(fieldK);

            DisjunctionQuery dis1 = SearchQuery.disjuncts(at, b1t);
            DisjunctionQuery dis2 = SearchQuery.disjuncts(ct, ht);
            DisjunctionQuery dis3 = SearchQuery.disjuncts(b2t, kt);

            ConjunctionQuery conjSQ = SearchQuery.conjuncts(dis1, dis2, dis3);
            queryList.add(new SearchQuery(indexName, conjSQ).limit(limit));

        }

        return queryList;
    }


    public List<SearchQuery> build1BDDDCDDDQueries(int limit, String indexName) {
        List<SearchQuery>  queryList = new ArrayList<>();
        String fieldF = fieldsMap.get("f");
        String fieldE = fieldsMap.get("e");
        String fieldG = fieldsMap.get("g");
        String fieldD = fieldsMap.get("d");
        String fieldH = fieldsMap.get("h");
        String fieldC = fieldsMap.get("c");
        String fieldB = fieldsMap.get("b");

        int lenF = valuesMap.get("f").size();
        int lenE = valuesMap.get("e").size();
        int lenG = valuesMap.get("g").size();
        int lenD = valuesMap.get("d").size();
        int lenH = valuesMap.get("h").size();
        int lenC = valuesMap.get("c").size();
        int lenB = valuesMap.get("b").size();

        for (int i = 0; i< ITEMS; i++) {
            String valF = valuesMap.get("f").get(rand.nextInt(lenF));
            String valE = valuesMap.get("e").get(rand.nextInt(lenE));
            String valG1 = valuesMap.get("g").get(rand.nextInt(lenG));
            String valD = valuesMap.get("d").get(rand.nextInt(lenD));
            String valH = valuesMap.get("h").get(rand.nextInt(lenH));
            String valC = valuesMap.get("c").get(rand.nextInt(lenC));
            String valG2 = valuesMap.get("g").get(rand.nextInt(lenG));
            String valB = valuesMap.get("b").get(rand.nextInt(lenB));

            TermQuery ft = SearchQuery.term(valF).field(fieldF);
            TermQuery et = SearchQuery.term(valE).field(fieldE);
            TermQuery g1t = SearchQuery.term(valG1).field(fieldG);
            TermQuery dt = SearchQuery.term(valD).field(fieldD);
            BooleanFieldQuery ht = SearchQuery.booleanField(Boolean.parseBoolean(valH)).field(fieldH);
            TermQuery ct = SearchQuery.term(valC).field(fieldC);
            TermQuery g2t = SearchQuery.term(valG2).field(fieldG);
            TermQuery bt = SearchQuery.term(valB).field(fieldB);

            DisjunctionQuery dis1 = SearchQuery.disjuncts(ft, et, g1t, dt);
            DisjunctionQuery dis2 = SearchQuery.disjuncts(ht, ct, g2t, bt);

            ConjunctionQuery conjSQ = SearchQuery.conjuncts(dis1, dis2);
            queryList.add(new SearchQuery(indexName, conjSQ).limit(limit));
        }
        return queryList;
    }
}
