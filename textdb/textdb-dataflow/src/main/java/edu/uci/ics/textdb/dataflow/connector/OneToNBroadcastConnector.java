package edu.uci.ics.textdb.dataflow.connector;

import java.util.ArrayList;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IConnector;
import edu.uci.ics.textdb.api.dataflow.IOperator;

/**
 * OneToNBroadcastConnector connects one input operator with multiple output operators.
 * The tuples from the input operator will be broadcast to every output operator.
 * 
 * It is required that all output operators need to be opened prior to calling getNextTuple().
 * @author Zuozhi Wang (zuozhiw)
 *
 */
public class OneToNBroadcastConnector implements IConnector {
    
    private int outputOperatorNumber;
    
    // A list of all child output operators
    private ArrayList<IOperator> outputOperatorList;
    // A list to maintain cursors of all operators
    private ArrayList<Integer> outputCursorList;
    // A list to maintain operators' status (opened or closed)
    private ArrayList<Boolean> outputStatusList;
    private boolean inputOperatorOpened;
    
    private IOperator inputOperator;
    // an in-memory list to cache tuples from input tuple, see getNextTuple() for more details
    private ArrayList<ITuple> inputTupleList;
    
    /**
     * Constructs a OneToNBroadcastConnector with n output operators.
     * @param n, the number of output operators this connector has
     */
    public OneToNBroadcastConnector(int n) {        
        this.inputTupleList = new ArrayList<>();
        inputOperatorOpened = false;
        this.outputOperatorNumber = n;
        intializeOutputOperators();
    }
    
    private void intializeOutputOperators() {
        this.outputOperatorList = new ArrayList<>();
        this.outputCursorList = new ArrayList<>();
        this.outputStatusList = new ArrayList<>();
        
        for (int i = 0; i < this.outputOperatorNumber; i++) {
            outputCursorList.add(-1);
            outputStatusList.add(false);
            outputOperatorList.add(new ConnectorOutputOperator(this, i));
        }
    }
    
    /**
     * Get the total number of output operators this connector has.
     */
    @Override
    public int getOutputNumber() {
        return outputOperatorNumber;
    }
    
    /**
     * Get the output operator corresponding to the index.
     * Index starts from 0. 
     * 0 corresponds to the first output operator, 1 corresponds to the second, etc.
     * 
     * Return null if outputIndex is out of bound.
     */
    @Override
    public IOperator getOutputOperator(int outputIndex) {
        if (outputIndex < outputOperatorNumber) {
            return this.outputOperatorList.get(outputIndex);
        } else {
            return null;
        }
    }
        
    /*
     * This returns the nextTuple of the operator corresponding to the index.
     * A cursor will be maintained for each operator. 
     * Tuples from input operators are cached in an in-memory list.
     * A new tuple will be fetched from input operator whenever a cursor exceeds the list size.
     */
    private ITuple getNextTuple(int outputOperatorIndex) throws Exception {
        int nextPosition = outputCursorList.get(outputOperatorIndex) + 1;
        outputCursorList.set(outputOperatorIndex, nextPosition);
        
        if (nextPosition < inputTupleList.size()) {
            return inputTupleList.get(nextPosition);
        } else {
            ITuple nextInputTuple = inputOperator.getNextTuple();
            if (nextInputTuple == null) {
                return null;
            } else {
                inputTupleList.add(nextInputTuple);
                return nextInputTuple;
            }
        }
    }
    
    private void openInputOperator(int outputOperatorIndex) throws Exception {
        outputStatusList.set(outputOperatorIndex, true);
        if (! inputOperatorOpened) {
            inputOperator.open();
            inputOperatorOpened = true;
        }
    }
    
    private void closeInputOperator(int outputOperatorIndex) throws Exception {
        outputStatusList.set(outputOperatorIndex, false);
        boolean isAllClosed = isAllOutputOperatorClosed();
        if (isAllClosed) {
            inputOperator.close();
        }
    }
    
    /**
     * Set the input operator of this connector
     * @param inputOperator
     */
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }
    
    public IOperator getInputOperator() {
        return this.inputOperator;
    }

    private boolean isAllOutputOperatorClosed() {
        return ! outputStatusList.stream().reduce(false, (a, b) -> (a || b));
    }
    
    
    private class ConnectorOutputOperator implements IOperator {
        
        private OneToNBroadcastConnector ownerConnector;
        private int outputIndex;
        
        private ConnectorOutputOperator(OneToNBroadcastConnector ownerConnector, int outputIndex) {
            this.ownerConnector = ownerConnector;
            this.outputIndex = outputIndex;
        }

        @Override
        public void open() throws Exception {
            ownerConnector.openInputOperator(outputIndex);
        }

        @Override
        public ITuple getNextTuple() throws Exception {
            return ownerConnector.getNextTuple(outputIndex);
        }

        @Override
        public void close() throws Exception {      
            ownerConnector.closeInputOperator(outputIndex);
        }

        @Override
        public Schema getOutputSchema() {
            return ownerConnector.getInputOperator().getOutputSchema();
        }
        
    }
   
}
