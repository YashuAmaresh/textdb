package edu.uci.textdb.framework.api;

import java.util.Arrays;

public class Test {
    
    
    public static void main(String[] args) throws Exception {
        
        DataTuple tuple = new DataTuple(new TextField("test"));
        
        TextField textField = tuple.getField();
        String text = textField.getValue();
        
        String text2 = tuple.getField(TextField.class).getValue();
        
        
        
    }
    

}
