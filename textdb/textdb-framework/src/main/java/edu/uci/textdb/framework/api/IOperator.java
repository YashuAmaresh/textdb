package edu.uci.textdb.framework.api;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;

/**
 * IOperator is the fundamental interface for all operator classes.
 * Each operator will implement:
 *   setInputOperator(IOperator... operators)
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
