package edu.uci.ics.textdb.dataflow.source.file;

import java.util.List;

import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IPredicate;

public class FileSourcePredicate implements IPredicate {

    private List<String> filePathList;
    private String fieldName;
    private FieldType fieldType;
    
    public FileSourcePredicate(List<String> filePathList, String fieldName, FieldType fieldType) {
        this.filePathList = filePathList;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }
    
    public List<String> getFilePathList() {
        return filePathList;
    }

    public String getFieldName() {
        return fieldName;
    }

    public FieldType getFieldType() {
        return fieldType;
    }
    
}
