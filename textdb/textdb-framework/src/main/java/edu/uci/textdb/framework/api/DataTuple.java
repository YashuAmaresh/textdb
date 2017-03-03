package edu.uci.textdb.framework.api;

import java.util.List;

public class DataTuple {
    
    private TextField field;
    
    public DataTuple(TextField field) {
        this.field = field;
    }
    
    
    
    public <T extends IField> T getField() {
        try {
            @SuppressWarnings("unchecked")
            T returnField = (T) this.field;
            return returnField;
        } catch (ClassCastException e) {
            throw new RuntimeException("catched");
        }
    }
    
    public <T extends IField> T getField(Class<T> fieldType) {
        try {
            @SuppressWarnings("unchecked")
            T returnField = (T) this.field;
            return returnField;
        } catch (ClassCastException e) {
            throw new RuntimeException("catched");
        }
    }

}
