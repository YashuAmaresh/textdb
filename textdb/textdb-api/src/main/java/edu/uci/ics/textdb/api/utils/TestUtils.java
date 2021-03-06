package edu.uci.ics.textdb.api.utils;

import java.util.List;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.tuple.Tuple;

/**
 * @author sandeepreddy602
 * @author zuozhi
 * @author rajeshyarlagadda
 */
public class TestUtils {

    /**
     * Returns true if the tupleList contains a tuple.
     * 
     * Since we only want to compare the content of two tuples, _ID and PAYLOAD are two fields 
     *   that can be safely ignored because these two fields are automatically generated by the system.
     * 
     * @param tupleList
     * @param containsTuple
     * @return
     */
    public static boolean contains(List<Tuple> tupleList, Tuple containsTuple) {
        tupleList = Utils.removeFields(tupleList, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        containsTuple = Utils.removeFields(containsTuple, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        
        return tupleList.contains(containsTuple);
    }
    
    /**
     * Returns true if the tupleList contains a list of tuples.
     * 
     * Since we only want to compare the content of two tuples, _ID and PAYLOAD are two fields 
     *   that can be safely ignored because these two fields are automatically generated by the system.
     * 
     * @param tupleList
     * @param containsTupleList
     * @return
     */
    public static boolean containsAll(List<Tuple> tupleList, List<Tuple> containsTupleList) {
        tupleList = Utils.removeFields(tupleList, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        containsTupleList = Utils.removeFields(containsTupleList, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        
        return tupleList.containsAll(containsTupleList);
    }
    
    /**
     * Returns true if the two tuple lists are equivalent (order doesn't matter)
     * 
     * Since we only want to compare the content of two tuples, _ID and PAYLOAD are two fields 
     *   that can be safely ignored because these two fields are automatically generated by the system.
     * 
     * @param expectedResults
     * @param exactResults
     * @return
     */
    public static boolean equals(List<Tuple> expectedResults, List<Tuple> exactResults) {
        expectedResults = Utils.removeFields(expectedResults, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        exactResults = Utils.removeFields(exactResults, SchemaConstants._ID, SchemaConstants.PAYLOAD);

        if (expectedResults.size() != exactResults.size())
            return false;
        
        return expectedResults.containsAll(exactResults) && exactResults.containsAll(expectedResults);
    }
    
}
