package edu.uci.textdb.framework.api;

public class TextField implements IField {
    
    private String text;
    
    public TextField(String text) { 
        this.text = text;
    }
    
    @Override
    public String getValue() {
        return text;
    }

}
