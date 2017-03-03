package edu.uci.textdb.framework.api;

public class Attribute {
    
    private String fieldName;
    private Class<? extends IField> fieldType;
    
    public Attribute(String fieldName, Class<? extends IField> fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }
    
    public String getFieldName() {
        return this.fieldName;
    }
    
    public Class<? extends IField> getFieldType() {
        return this.fieldType;
    }

}
