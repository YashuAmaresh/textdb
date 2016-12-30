package edu.uci.ics.textdb.perftest.fuzzytokenmatcher;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.uci.ics.textdb.api.exception.TextDBException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataStore;

/**
 * @author Qing Tang
 * @author Hailey Pan
 * 
 *         This is the performance test of fuzzy token operator.
 */

public class FuzzyTokenMatcherPerformanceTest {

    private static String HEADER = "Date,Record #, Threshold,Min, Max, Average, Std, Average Results,Commit Number";
    private static String delimiter = ",";
    private static String newLine = "\n";

    private static List<Double> timeResults = null;
    private static int totalResultCount = 0;
    private static boolean bool = true;
    private static String csvFile = "fuzzytoken.csv";

    /*
     * queryFileName contains line(s) of queries; the file must be placed in
     * ./perftest-files/queries/
     * 
     * thresholds is a list of thresholds
     * 
     * This function will match the queries against all indices in
     * ./index/standard/
     * 
     * Test results includes minimum runtime, maximum runtime, average runtime,
     * the standard deviation and the average results each threshold. They are
     * written in a csv file ./perftest-files/results/fuzzytoken.csv
     * 
     * CSV file example:
     * 
     * Date,                Record #,       Threshold, Min,  Max,     Average, Std,    Average Results, Commit Number
     * 09-09-2016 00:54:27, abstract_100,   0.8,       0.01, 0.128,   0.042,   0.0393, 8
     * 
     * Commit number is designed for performance dashboard. It will be appended
     * to the result file only when the performance test is run by
     * /textdb-scripts/dashboard/build.py
     * 
     */
    public static void runTest(String queryFileName, List<Double> thresholds)
            throws TextDBException, IOException {

        // Reads queries from query file into a list
        ArrayList<String> queries = PerfTestUtils.readQueries(PerfTestUtils.getQueryPath(queryFileName));
        FileWriter fileWriter = null;

        // Gets the current time
        String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());

        File indexFiles = new File(PerfTestUtils.standardIndexFolder);
        double avgTime = 0;
        for (double threshold : thresholds) {
            for (File file : indexFiles.listFiles()) {
                if (file.getName().startsWith(".")) {
                    continue;
                }
                DataStore dataStore = new DataStore(PerfTestUtils.getIndexPath(file.getName()),
                        MedlineIndexWriter.SCHEMA_MEDLINE);

                PerfTestUtils.createFile(PerfTestUtils.getResultPath(csvFile), HEADER);
                fileWriter = new FileWriter(PerfTestUtils.getResultPath(csvFile), true);
                fileWriter.append(newLine);
                fileWriter.append(currentTime + delimiter);
                fileWriter.append(file.getName() + delimiter);
                fileWriter.append(Double.toString(threshold) + delimiter);
                resetStats();
                match(queries, threshold, new StandardAnalyzer(), dataStore, bool);
                avgTime = PerfTestUtils.calculateAverage(timeResults);
                fileWriter.append(Collections.min(timeResults) + "," + Collections.max(timeResults) + "," + avgTime
                        + "," + PerfTestUtils.calculateSTD(timeResults, avgTime) + ","
                        + totalResultCount / queries.size());
                fileWriter.flush();
                fileWriter.close();

            }
        }
    }

    /*
     * reset timeResults and totalReusltCount
     */
    public static void resetStats() {
        timeResults = new ArrayList<Double>();
        totalResultCount = 0;
    }

    /*
     * This function does match for a list of queries
     */
    public static void match(ArrayList<String> queryList, double threshold, Analyzer luceneAnalyzer,
            DataStore dataStore, boolean bool) throws TextDBException, IOException {

        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);

        for (String query : queryList) {
            FuzzyTokenPredicate predicate = new FuzzyTokenPredicate(query, attributeNames, luceneAnalyzer,
                    threshold);
            FuzzyTokenMatcher fuzzyTokenMatcher = new FuzzyTokenMatcher(predicate);

            IndexBasedSourceOperator indexSource = new IndexBasedSourceOperator(
                    predicate.getDataReaderPredicate(dataStore));

            fuzzyTokenMatcher.setInputOperator(indexSource);

            long startMatchTime = System.currentTimeMillis();
            fuzzyTokenMatcher.open();
            int counter = 0;
            ITuple nextTuple = null;
            while ((nextTuple = fuzzyTokenMatcher.getNextTuple()) != null) {
                List<Span> spanList = ((ListField<Span>) nextTuple.getField(SchemaConstants.SPAN_LIST)).getValue();
                counter += spanList.size();
            }
            long endMatchTime = System.currentTimeMillis();
            double matchTime = (endMatchTime - startMatchTime) / 1000.0;

            timeResults.add(Double.parseDouble(String.format("%.4f", matchTime)));
            totalResultCount += counter;
        }

    }

}
