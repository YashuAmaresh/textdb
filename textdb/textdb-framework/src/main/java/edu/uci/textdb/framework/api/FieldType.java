package edu.uci.textdb.framework.api;

public enum FieldType {
    TEXT   (TextField.class),
    STRING (StringField.class);
    
    private final Class<? extends IField> fieldType;
    
    FieldType(Class<? extends IField> fieldType) {
        this.fieldType = fieldType;
    }
    
    public Class<? extends IField> getFieldType() {
        return fieldType;
    }
    

}
