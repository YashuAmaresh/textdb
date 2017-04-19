package edu.uci.ics.textdb.perftest.twitter;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

public class TwitterSample {
    
    public static String twitterFilePath = "/Users/georgewang/Desktop/twitter_data/data/climate_change_tweets.json";
    public static String twitterClimateTable = "twitter_climate";
    
    public static void main(String[] args) throws Exception {
        writeTwitterIndex();
    }
    
    public static void writeTwitterIndex() throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(twitterClimateTable);
        relationManager.createTable(twitterClimateTable, "../index/twitter_climate/", TwitterSchema.TWITTER_SCHEMA, 
                LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter dataWriter = relationManager.getTableDataWriter(twitterClimateTable);
        dataWriter.open();
        
        int counter = 0;
        JsonNode jsonNode = new ObjectMapper().readTree(new File(twitterFilePath));
        for (JsonNode tweet : jsonNode) {
            try {
                int id = tweet.get("id").asInt();
                String createAt = tweet.get("create_at").asText();
                String userName = tweet.get("user").get("screen_name").asText();
                String text = tweet.get("text").asText();
                Tuple tuple = new Tuple(TwitterSchema.TWITTER_SCHEMA,
                        new IntegerField(id),
                        new StringField(createAt),
                        new StringField(userName),
                        new TextField(text));
                dataWriter.insertTuple(tuple);
                counter++;
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
        }
        
        dataWriter.close();
        System.out.println("write twitter data finished");
        System.out.println(counter + " tweets written");
    }

}
