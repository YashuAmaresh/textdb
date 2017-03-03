package edu.uci.textdb.framework.api;

public class StringField implements IField {
    
    private final String str;
    
    public StringField(String str) {
        this.str = str;
    }
    
    @Override
    public String getValue() {
        return str;
    }

}
