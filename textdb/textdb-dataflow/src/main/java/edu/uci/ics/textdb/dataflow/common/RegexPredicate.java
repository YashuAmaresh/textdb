package edu.uci.ics.textdb.dataflow.common;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexToGramQueryTranslator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 * This class is responsible for calculating regex match. <br>
 * It uses RegexToGramQueryTranslator to translate a regex to a query in lucene.
 * <br>
 * 
 * @author Zuozhi Wang
 * @author Shuying Lai
 *
 */
public class RegexPredicate implements IPredicate {

    private String regex;
    private List<Attribute> attributeList;

    private Analyzer luceneAnalyzer;


    public RegexPredicate(String regex, List<Attribute> attributeList, Analyzer analyzer) throws DataFlowException {
        this.regex = regex;
        this.luceneAnalyzer = analyzer;
        this.attributeList = attributeList;
    }



    public DataReaderPredicate generateDataReaderPredicate(IDataStore dataStore) throws DataFlowException {
        Query luceneQuery;
        String queryString;
        List<String> fieldNameList = attributeList.stream()
                .filter(attr -> (attr.getFieldType() == FieldType.TEXT || attr.getFieldType() == FieldType.STRING))
                .map(attr -> attr.getFieldName()).collect(Collectors.toList());
        
        // Try to apply translator. If it fails, use scan query.
        try {
            queryString = RegexToGramQueryTranslator.translate(regex).getLuceneQueryString();
        } catch (com.google.re2j.PatternSyntaxException e) {
            queryString = DataConstants.SCAN_QUERY;
        }

        try {
            luceneQuery = generateLuceneQuery(fieldNameList, queryString);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
        
        DataReaderPredicate predicate = new DataReaderPredicate(luceneQuery, queryString, dataStore, attributeList,
                luceneAnalyzer);
        predicate.setIsSpanInformationAdded(false);
        return predicate;
    }
    
    private Query generateLuceneQuery(List<String> fields, String queryStr) throws ParseException {
        String[] fieldsArray = new String[fields.size()];
        QueryParser parser = new MultiFieldQueryParser(fields.toArray(fieldsArray), luceneAnalyzer);
        return parser.parse(queryStr);
    }

    public String getRegex() {
        return regex;
    }

    public Analyzer getLuceneAnalyzer() {
        return this.luceneAnalyzer;
    }

    public List<Attribute> getAttributeList() {
        return attributeList;
    }

}
