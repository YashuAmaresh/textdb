package edu.uci.ics.textdb.dataflow.source.file;

import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IPredicate;

public class FileSourcePredicate implements IPredicate {

    private String filePath;
    private String fieldName;
    private FieldType fieldType;
    
    public FileSourcePredicate(String filePath, String fieldName, FieldType fieldType) {
        this.filePath = filePath;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }
    
    public String getFilePath() {
        return filePath;
    }

    public String getFieldName() {
        return fieldName;
    }

    public FieldType getFieldType() {
        return fieldType;
    }
    
}
