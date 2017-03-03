package edu.uci.textdb.framework.api;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;

/**
 * IOperator is the fundamental interface for all operator classes.
 * Each operator will implement:
 *   getInputArity:    describes how many input operators this operator needs
 *   
 *   setInputOperator: sets one or multiple operators as this operator's input operator(s)
 *                       the number of operators it receives must be consistent with getInputArity()
 *   
 *   open:             opens the input operator(s), sets up the output schema, 
 *                       and sets up resources if needed
 *   
 *   getNextTuple:     pulls the tuples from input operator(s) if needed, 
 *                       and returns the next tuple this operator generates.
 *   
 *   close:            closes the input operator(s) and close resources if needed
 *   
 *   getOutputSchema:  returns the output schema of this operator
 *   
 * 
 * @author Zuozhi Wang
 * Created on 03/01/2017
 *
 */
public interface IOperator {
        
    int getInputArity();
    
    void setInputOperator(IOperator... operators) throws TextDBException;

    void open() throws TextDBException;
    
    ITuple getNextTuple() throws TextDBException;
    
    void close() throws TextDBException;
    
    Schema getOutputSchema();

}
