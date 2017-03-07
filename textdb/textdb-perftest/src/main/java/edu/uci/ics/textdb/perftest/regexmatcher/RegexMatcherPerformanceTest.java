package edu.uci.ics.textdb.perftest.regexmatcher;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.exception.TextDBException;
import org.apache.lucene.analysis.Analyzer;
import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcherSourceOperator;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;

/*
 * 
 * @author Zuozhi Wang
 * @author Hailey Pan
 * 
 */
public class RegexMatcherPerformanceTest {

    public static int resultNumber;
    private static String HEADER = "Date, dataset, Average Time, Average Results, Commit Number";
    private static String delimiter = ",";
    private static double totalMatchingTime = 0.0;
    private static int totalRegexResultCount = 0;
    private static String csvFile  = "regex.csv";

    /*
     * regexQueries is a list of regex queries.
     * 
     * This function will match the queries against all indices in
     * ./index/trigram/
     * 
     * Test results includes the average runtime of all queries, the average
     * number of results. These results are written to
     * ./perftest-files/results/regex.csv.
     * 
     * CSV file example: 
     * Date,                dataset,      Average Time, Average Results, Commit Number
     * 09-09-2016 00:54:29, abstract_100, 0.2798,       69.80
     * 
     * Commit number is designed for performance dashboard. It will be appended
     * to the result file only when the performance test is run by
     * /textdb-scripts/dashboard/build.py
     * 
     */
    public static void runTest(List<String> regexQueries)
            throws TextDBException, IOException {

        FileWriter fileWriter = null;
         
        // Gets the current time for naming the cvs file
        String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());

        // Writes results to the csv file
        File indexFiles = new File(PerfTestUtils.trigramIndexFolder);
   
        for (File file : indexFiles.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            String tableName = file.getName().replace(".txt", "") + "_trigram";

            PerfTestUtils.createFile(PerfTestUtils.getResultPath(csvFile), HEADER);
            fileWriter = new FileWriter(PerfTestUtils.getResultPath(csvFile),true);
            matchRegex(regexQueries, tableName);
            fileWriter.append("\n");
            fileWriter.append(currentTime + delimiter);
            fileWriter.append(file.getName() + delimiter);
            fileWriter.append(String.format("%.4f", totalMatchingTime / regexQueries.size()));
            fileWriter.append(delimiter);
            fileWriter.append(String.format("%.2f", totalRegexResultCount * 1.0 / regexQueries.size()));
            fileWriter.flush();
            fileWriter.close();
        }
   
    }

    /*
     *         This function does match for a list of regex queries
     */
    public static void matchRegex(List<String> regexes, String tableName) throws TextDBException, IOException {

        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);
        
        for(String regex: regexes){
	        // analyzer should generate grams all in lower case to build a lower
	        // case index.
	        Analyzer luceneAnalyzer = LuceneAnalyzerConstants.getNGramAnalyzer(3);
	        RegexPredicate regexPredicate = new RegexPredicate(regex, attributeNames, luceneAnalyzer);
	        
	        RegexMatcherSourceOperator regexSource = new RegexMatcherSourceOperator(regexPredicate, tableName);
	
	        long startMatchTime = System.currentTimeMillis();
	        regexSource.open();
	        int counter = 0;
	        Tuple nextTuple = null;
	        while ((nextTuple = regexSource.getNextTuple()) != null) {
	            List<Span> spanList = ((ListField<Span>) nextTuple.getField(SchemaConstants.SPAN_LIST)).getValue();
	            counter += spanList.size();
	        }
	        regexSource.close();
	        long endMatchTime = System.currentTimeMillis();
	        double matchTime = (endMatchTime - startMatchTime) / 1000.0;
	        totalMatchingTime += matchTime;
	        totalRegexResultCount += counter;
        }
    }

}