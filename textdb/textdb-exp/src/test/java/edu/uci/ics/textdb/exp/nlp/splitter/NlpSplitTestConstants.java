package edu.uci.ics.textdb.exp.nlp.splitter;

import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

public class NlpSplitTestConstants {
    
    public static String TEXT = "text";
    
    public static Attribute TEXT_ATTRIBUTE = new Attribute("text", AttributeType.TEXT);
    
    public static Schema SPLIT_SCHEMA = new Schema(TEXT_ATTRIBUTE);
    
    public static Tuple MULTI_SENTENCE_TUPLE = new Tuple(SPLIT_SCHEMA, 
            new TextField("Programming is so fun. TextDB uses Java. Bugs are always annoying."));
    
}
