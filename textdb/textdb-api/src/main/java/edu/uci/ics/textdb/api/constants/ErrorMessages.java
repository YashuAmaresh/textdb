/**
 * 
 */
package edu.uci.ics.textdb.api.constants;

import java.util.List;

import edu.uci.ics.textdb.api.schema.Schema;

/**
 * @author sandeepreddy602
 *
 */
public class ErrorMessages {
    public static final String OPERATOR_NOT_OPENED = "The operator is not opened";
    public static final String SCHEMA_CANNOT_BE_NULL = "Schema cannot be null or empty";
    public static final String INPUT_OPERATOR_NOT_SPECIFIED = "Input operator is not specified";
    public static final String INPUT_OPERATOR_CHANGED_AFTER_OPEN = "Input Operator cannot be changed after opening the operator";


    public static final String DUPLICATE_ATTRIBUTE(String attributeName, Schema schema) {
        return DUPLICATE_ATTRIBUTE(attributeName, schema);
    }
    
    public static final String DUPLICATE_ATTRIBUTE(String attributeName, List<String> attributeNameList) {
        return String.format("attribute %s is already in the existing attributes: %s", 
                attributeName, attributeNameList);
    }
    
}
