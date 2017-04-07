package edu.uci.ics.textdb.perftest.sample;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.exp.regexsplit.RegexSplitOperator;
import edu.uci.ics.textdb.exp.regexsplit.RegexSplitPredicate;
import edu.uci.ics.textdb.exp.source.TupleStreamSourceOperator;
import edu.uci.ics.textdb.dataflow.sink.TupleStreamSink;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;
import edu.uci.ics.textdb.perftest.promed.PromedSchema;
import edu.uci.ics.textdb.perftest.zhangschema.ZhangSchema;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

public class ChineseExtractor {
        public static final String ZHANG_SAMPLE_TABLE = "zhang";
    
    public static String zhangFilesDirectory;
    public static String zhangIndexDirectory;
    public static String sampleDataFilesDirectory;

    static {
        try {
            // Finding the absolute path to the sample data files directory and index directory

            // Checking if the resource is in a jar
            String referencePath = ChineseExtractor.class.getResource("").toURI().toString();
            if(referencePath.substring(0, 3).equals("jar")) {
                zhangFilesDirectory = "../textdb-perftest/src/main/resources/sample-data-files/zhang/";
                zhangIndexDirectory = "../textdb-perftest/src/main/resources/index/standard/zhang/";
                sampleDataFilesDirectory = "../textdb-perftest/src/main/resources/sample-data-files/";
            }
            else {
                zhangFilesDirectory = Paths.get(ChineseExtractor.class.getResource("/sample-data-files/zhang")
                        .toURI())
                        .toString();
                System.out.println(zhangFilesDirectory);
                zhangIndexDirectory = Paths.get(ChineseExtractor.class.getResource("/index/standard")
                        .toURI())
                        .toString() + "/zhang";
                sampleDataFilesDirectory = Paths.get(ChineseExtractor.class.getResource("/sample-data-files")
                        .toURI())
                        .toString();
                System.out.println("Flies location:  "+zhangFilesDirectory);
            }
        }
        catch(URISyntaxException | FileSystemNotFoundException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws Exception {
        // write the index of data files
        // index only needs to be written once, after the first run, this function can be commented out
        writeSampleIndex();

        // perform the extraction task
        chinesePlanTest1();
    }
    
    public static Tuple parseWholeContent(String fileName, String content) {
        try {
            Tuple tuple = new Tuple(ZhangSchema.ZHANG_SCHEMA, new StringField(fileName), new TextField(content));
            return tuple;
        } catch (Exception e) {
            return null;
        }
    }
    
    public static void showTableContentFromIndex(String table) throws Exception {
        // read index
        DataReader dataReader = RelationManager.getRelationManager().getTableDataReader(
                table, new MatchAllDocsQuery());
        List<Tuple> results = new ArrayList<>();
        Tuple tuple;
        
        dataReader.open();
        while ((tuple = dataReader.getNextTuple()) != null) {
            results.add(tuple);
        }
        
        System.out.println(DataflowUtils.getTupleListString(results));
        dataReader.close();
    }
    
    public static void writeSampleIndex() throws Exception {
        // construct file tuples: 1 file for 1 tuple.
        File sourceFileFolder = new File(zhangFilesDirectory);
        ArrayList<Tuple> fileTuples = new ArrayList<>();
        for (File htmlFile : sourceFileFolder.listFiles()) {
            StringBuilder sb = new StringBuilder();
            Scanner scanner = new Scanner(htmlFile);
            while (scanner.hasNext()) {
                sb.append(scanner.nextLine());
                sb.append("\n");
            }
            scanner.close();
            Tuple tuple = parseWholeContent(htmlFile.getName(), sb.toString());
            
            if (tuple != null) {
                fileTuples.add(tuple);
            }
        }
        // Construct a tuple source using the tuple.
        TupleStreamSourceOperator srcTuple = new TupleStreamSourceOperator(fileTuples, ZhangSchema.ZHANG_SCHEMA);
        
        ProjectionOperator project = new ProjectionOperator(new ProjectionPredicate(ZhangSchema.ZHANG_SCHEMA.getAttributeNames()));
        project.setInputOperator(srcTuple);
        
        // Split this tuple source into multiple tuples. Then write it into table.
        String splitRegex = "（(一|二|三|四|五|六)）";
        RegexSplitOperator regexSplit = new RegexSplitOperator(
                new RegexSplitPredicate(splitRegex, ZhangSchema.CONTENT, 
                        RegexSplitPredicate.SplitType.GROUP_RIGHT));
        regexSplit.setInputOperator(project);
        
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(ZHANG_SAMPLE_TABLE);
        relationManager.createTable(ZHANG_SAMPLE_TABLE, zhangIndexDirectory, 
                ZhangSchema.ZHANG_SCHEMA, LuceneAnalyzerConstants.chineseAnalyzerString());
        
        Tuple tuple;
        DataWriter dataWriter = relationManager.getTableDataWriter(ZHANG_SAMPLE_TABLE);
        dataWriter.open();
        regexSplit.open();
        while((tuple = regexSplit.getNextTuple()) != null) {
            Tuple newTuple = new Tuple(ZhangSchema.ZHANG_SCHEMA, 
                    new StringField(tuple.getField(ZhangSchema.ID).toString()),
                    new TextField(tuple.getField(ZhangSchema.CONTENT).toString()));
            dataWriter.insertTuple(newTuple);
        }
        regexSplit.close();
        dataWriter.close();
        
        // Output Tuple in the table
        showTableContentFromIndex(ZHANG_SAMPLE_TABLE);
    }
    
    public static void chinesePlanTest1() throws Exception {
        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(
                new KeywordPredicate("项目", Arrays.asList(PromedSchema.CONTENT),
                        new SmartChineseAnalyzer(), KeywordMatchingType.CONJUNCTION_INDEXBASED), ZHANG_SAMPLE_TABLE);
        
        ProjectionOperator projectSpanList = new ProjectionOperator(new ProjectionPredicate(Arrays.asList("_id", "id", "content")));
        
        RegexMatcher regexProject = new RegexMatcher(
                new RegexPredicate("项目来源.{0,30}\n", Arrays.asList("content"), new StandardAnalyzer()));
        
        RegexMatcher regexDuration = new RegexMatcher(
                new RegexPredicate("起止年限.{0,30}\n", Arrays.asList("content"), new StandardAnalyzer()));
        
        RegexMatcher regexHost = new RegexMatcher(
                new RegexPredicate("主持人.{0,30}\n", Arrays.asList("content"), new StandardAnalyzer()));
        
        Join joinProjectHost = new Join(new JoinDistancePredicate("content", 200));
        
        Join joinProjectHostDuration = new Join(new JoinDistancePredicate("content", 200));
        
        TupleStreamSink sink = new TupleStreamSink();
        
        projectSpanList.setInputOperator(keywordSource);
        
        OneToNBroadcastConnector connector = new OneToNBroadcastConnector(3);
        connector.setInputOperator(projectSpanList);
        
        regexProject.setInputOperator(connector.getOutputOperator(0));
        regexHost.setInputOperator(connector.getOutputOperator(1));
        regexDuration.setInputOperator(connector.getOutputOperator(2));
        
//        regexProject.setInputOperator(projectSpanList);
//        regexDuration.setInputOperator(projectSpanList);
//        regexHost.setInputOperator(projectSpanList);
        
        joinProjectHost.setInnerInputOperator(regexProject);
        joinProjectHost.setOuterInputOperator(regexHost);
        
        joinProjectHostDuration.setInnerInputOperator(regexDuration);
        joinProjectHostDuration.setOuterInputOperator(joinProjectHost);
        
        System.out.println("regex project is : " + regexProject.toString());
        System.out.println("regex host is : " + regexHost.toString());
        System.out.println("regex duration is : " + regexDuration.toString());
        System.out.println("join project host is: " + joinProjectHost.toString());
        System.out.println("join project host duration is: " + joinProjectHostDuration.toString());
        
        sink.setInputOperator(joinProjectHostDuration);
        sink.open();
        List<Tuple> results = sink.collectAllTuples();
        sink.close();
        
//        System.out.println(Utils.getTupleListString(results));
        System.out.println("total number of results: " + results.size());
    }
}