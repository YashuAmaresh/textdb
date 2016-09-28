package edu.uci.ics.textdb.plangen;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.constants.OperatorConstants;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.textdb.dataflow.join.Join;

/**
 * A graph of operators representing the query plan.
 * 
 * @author Zuozhi Wang
 */
public class OperatorGraph {
    
    // a map of an operator ID to the operator object
    HashMap<String, IOperator> operatorObjectMap;
    
    // a map of an operator ID to the operator's type
    HashMap<String, String> operatorTypeMap;
    // a map of an operator ID to the operator's properties
    HashMap<String, Map<String, String>> operatorPropertyMap;
    // a map of an operator ID to operator's outputs (a set of operator IDs)
    HashMap<String, HashSet<String>> adjacencyList;
    
    // a map of a Join Operator to its two input operators
    HashMap<Join, ArrayList<Object>> joinOperatorLinkMap;

    
    public OperatorGraph() {
        operatorObjectMap = new HashMap<>();
        operatorTypeMap = new HashMap<>();
        operatorPropertyMap = new HashMap<>();
        adjacencyList = new HashMap<>();
        joinOperatorLinkMap = new HashMap<>();
    }
    
    /**
     * Adds an operator to the operator graph.
     * 
     * @param operatorID
     * @param operatorType
     * @param operatorProperties
     * @throws PlanGenException, if the operator ID already exists or the operator type is invalid.
     */
    public void addOperator(String operatorID, String operatorType, Map<String, String> operatorProperties) throws PlanGenException {
        PlanGenUtils.planGenAssert(operatorID != null, "operatorID is null");
        PlanGenUtils.planGenAssert(operatorType != null, "operatorType is null");
        PlanGenUtils.planGenAssert(operatorProperties != null, "operatorProperties is null");
        
        PlanGenUtils.planGenAssert(! operatorID.trim().isEmpty(), "operatorID is empty");
        PlanGenUtils.planGenAssert(! operatorType.trim().isEmpty(), "operatorType is empty");
        
        PlanGenUtils.planGenAssert(! hasOperator(operatorID), "duplicate operatorID: "+operatorID);
        PlanGenUtils.planGenAssert(PlanGenUtils.isValidOperator(operatorType), 
                String.format("%s is an invalid operator type, it must be one of %s.", 
                        operatorType, OperatorConstants.operatorList.toString()));
  
        operatorTypeMap.put(operatorID, operatorType);
        operatorPropertyMap.put(operatorID, operatorProperties);
        adjacencyList.put(operatorID, new HashSet<>());   
    }
    
    /**
     * Adds a link from "src" operator to "dest" operator in the graph.
     * 
     * @param src, the operator ID of src operator
     * @param dest, the operator ID of dest operator
     * @throws PlanGenException, if the operator is null, is empty, or doesn't exist.
     */
    public void addLink(String src, String dest) throws PlanGenException {
        PlanGenUtils.planGenAssert(src != null, "src operator is null");
        PlanGenUtils.planGenAssert(dest != null, "dest operator is null");
        
        PlanGenUtils.planGenAssert(! src.trim().isEmpty(), "src operator is empty");
        PlanGenUtils.planGenAssert(! dest.trim().isEmpty(), "dest operator is empty");
        
        PlanGenUtils.planGenAssert(hasOperator(src), String.format("operator %s doesn't exist", src));
        PlanGenUtils.planGenAssert(hasOperator(dest), String.format("operator %s doesn't exist", dest));
        
        adjacencyList.get(src).add(dest);
    }
    
    /**
     * Returns true if the operator graph contains the operatorID.
     * 
     * @param operatorID
     * @return
     */
    public boolean hasOperator(String operatorID) {
        return adjacencyList.containsKey(operatorID);
    }
    
    /**
     * Builds and returns the query plan from the operator graph.
     * 
     * @return the plan generated from the operator graph
     * @throws Exception, if the operator grpah is invalid.
     */
    public Plan buildQueryPlan() throws Exception {
        buildOperators();        
        validateOperatorGraph();
        connectOperators();
        ISink sink = findSinkOperator();
        
        Plan queryPlan = new Plan(sink);
        return queryPlan;
    }

    /*
     * Builds the operators from the operators' properties to actual IOperator objects.
     * 
     */
    private void buildOperators() throws Exception {
        for (String operatorID : adjacencyList.keySet()) {
            String opeartorType = operatorTypeMap.get(operatorID);
            Map<String, String> operatorProperties = operatorPropertyMap.get(operatorID);
            
            IOperator operator = PlanGenUtils.buildOperator(opeartorType, operatorProperties);
            operatorObjectMap.put(operatorID, operator);
        }
    }
    
    /*
     * Validates the operator graph.
     * The operator graph must meet all of the following requirements:
     * 
     *   the graph is a DAG (directed acyclic graph)
     *     this DAG is weakly connected (no unreachable vertices).
     *     there's no cycles in this DAG.
     *   each operator must meet its input and output arity constraints.
     *   the operator graph has at least one source operator.
     *   the operator graph has exactly one sink.
     * 
     * Throws PlanGenException if the operator graph is invalid.
     */
    private void validateOperatorGraph() throws PlanGenException {
        checkGraphConnectivity();
        checkGraphCycle();
        checkOperatorInputArity();
        checkOperatorOutputArity();
        checkSourceOperator();
        checkSinkOperator();
    }
  
    /*
     * Detects if there are any operators not connected to the operator graph.
     * 
     * This function builds an undirected version of the operator graph, and then 
     *   uses a Depth First Search (DFS) algorithm to traverse the graph from any vertex.
     * If the graph is weakly connected, then every vertex should be reached after the traversal.
     * 
     * PlanGenException is thrown if there is an operator not connected to the operator graph.
     * 
     */
    private void checkGraphConnectivity() throws PlanGenException {
        HashMap<String, HashSet<String>> undirectedAdjacencyList = new HashMap<>();
        for (String vertex : adjacencyList.keySet()) {
            undirectedAdjacencyList.put(vertex, new HashSet<>(adjacencyList.get(vertex)));
        }
        for (String vertexOrigin : adjacencyList.keySet()) {
            for (String vertexDestination : adjacencyList.get(vertexOrigin)) {
                undirectedAdjacencyList.get(vertexDestination).add(vertexOrigin);
            }
        }
        
        String vertex = undirectedAdjacencyList.keySet().iterator().next();
        HashSet<String> unvisitedVertices = new HashSet<>(undirectedAdjacencyList.keySet());
        HashSet<String> visitedVertices = new HashSet<>();
        
        connectivityDfsVisit(vertex, undirectedAdjacencyList, unvisitedVertices, visitedVertices);
        
        if ((! unvisitedVertices.isEmpty()) || 
                visitedVertices.size() != adjacencyList.keySet().size()) {
            throw new PlanGenException("Operators: " + unvisitedVertices + " are not connected to the operator graph.");
        }   
    }
    
    /*
     * This is a helper function for checking connectivity by traversing the graph using DFS algorithm. 
     */
    private void connectivityDfsVisit(String vertex, HashMap<String, HashSet<String>> undirectedAdjacencyList, 
            HashSet<String> unvisitedVertices, HashSet<String> visitedVertices) {
        unvisitedVertices.remove(vertex);       
        for (String adjacentVertex : undirectedAdjacencyList.get(vertex)) {
            if (unvisitedVertices.contains(adjacentVertex)) {
                connectivityDfsVisit(adjacentVertex, undirectedAdjacencyList, unvisitedVertices, visitedVertices);
            }
        }
        visitedVertices.add(vertex);
    }
    
    /*
     * Detects if there are any cycles in the operator graph.
     * 
     * This function uses a Depth First Search (DFS) algorithm to traverse the graph.
     * It detects cycle by maintaining two lists of visited and visiting vertices,
     * during the traversal, if it reaches an vertex that is in the visiting list, then there's a cycle.
     * 
     * PlanGenException is thrown if a cycle is detected in the graph.
     * 
     */
    private void checkGraphCycle() throws PlanGenException {
        HashSet<String> unvisitedVertices = new HashSet<>(adjacencyList.keySet());
        HashSet<String> visitingVertices = new HashSet<>();
        
        for (String vertex : adjacencyList.keySet()) {
            if (unvisitedVertices.contains(vertex)) {
                checkCycleDfsVisit(vertex, unvisitedVertices, visitingVertices);
            }
        }
    }
    
    /*
     * This is a helper function for detecting cycles by traversing the graph the graph using DFS algorithm. 
     */
    private void checkCycleDfsVisit(String vertex, HashSet<String> unvisitedVertices, 
            HashSet<String> visitingVertices) throws PlanGenException {
        unvisitedVertices.remove(vertex);
        visitingVertices.add(vertex);
        
        for (String adjacentVertex : adjacencyList.get(vertex)) {
            if (visitingVertices.contains(adjacentVertex)) {
                throw new PlanGenException("The following operators form a cycle in operator graph: "+visitingVertices);
            }
            if (unvisitedVertices.contains(adjacentVertex)) {
                checkCycleDfsVisit(adjacentVertex, unvisitedVertices, visitingVertices);
            }
        }
        
        visitingVertices.remove(vertex);
    }
    
    /*
     * Checks if the input arities of all operators match the expected input arities.
     */
    private void checkOperatorInputArity() throws PlanGenException {
        HashMap<String, HashSet<String>> transposeAdjacencyList = new HashMap<>();
        for (String vertex : adjacencyList.keySet()) {
            transposeAdjacencyList.put(vertex, new HashSet<>());
        }
        for (String vertexOrigin : adjacencyList.keySet()) {
            for (String vertexDestination : adjacencyList.get(vertexOrigin)) {
                transposeAdjacencyList.get(vertexDestination).add(vertexOrigin);
            }
        }
        
        for (String vertex : transposeAdjacencyList.keySet()) {
            int actualInputArity = transposeAdjacencyList.get(vertex).size();
            int expectedInputArity = OperatorArityConstants.getFixedInputArity(operatorTypeMap.get(vertex));
            PlanGenUtils.planGenAssert(
                    actualInputArity == expectedInputArity,
                    String.format("Operator %s should have %d inputs, got %d.", vertex, expectedInputArity, actualInputArity));
        }
    }
    
    /*
     * Checks if the output arity of the operators match.
     * 
     * All operators (except sink) should have at least 1 outputs.
     * 
     * The linking operator phrase will automatically added a One to N Connector to
     * an operator with multiple outputs, so the outputs arities are not checked.
     * 
     */
    private void checkOperatorOutputArity() throws PlanGenException {
        for (String vertex : adjacencyList.keySet()) {
            int actualOutputArity = adjacencyList.get(vertex).size();
            int expectedOutputArity = OperatorArityConstants.getFixedOutputArity(operatorTypeMap.get(vertex));

            if (vertex.toLowerCase().contains("sink")) {
                PlanGenUtils.planGenAssert(
                        actualOutputArity == expectedOutputArity,
                        String.format("Sink %s should have %d output links, got %d.", vertex, expectedOutputArity, actualOutputArity));
            } else {
                PlanGenUtils.planGenAssert(
                        actualOutputArity != 0,
                        String.format("Operator %s should have at least %d output links, got 0.", vertex, expectedOutputArity)); 
            }
        }
    }
    
    /*
     * Checks that the operator graph has at least one source operator
     */
    private void checkSourceOperator() throws PlanGenException {
        boolean sourceExist = adjacencyList.keySet().stream()
                .map(operator -> operatorTypeMap.get(operator))
                .anyMatch(type -> type.toLowerCase().contains("source"));
        
        PlanGenUtils.planGenAssert(sourceExist, "There must be at least one source operator.");
    }
    
    /*
     * Checks that the operator graph has exactly one sink operator.
     */
    private void checkSinkOperator() throws PlanGenException {
        long sinkOperatorNumber = adjacencyList.keySet().stream()
                .map(operator -> operatorTypeMap.get(operator))
                .filter(operatorType -> operatorType.toLowerCase().contains("sink"))
                .count();
        
        PlanGenUtils.planGenAssert(sinkOperatorNumber == 1, 
                String.format("There must be exaxtly one sink operator, got %d.", sinkOperatorNumber));
    }
    
    /*
     * Connects IOperator objects together according to the operator graph.
     * 
     * This functions traverses the graph. For each link it encounters, it invokes
     * the corresponding "setInputOperator" function to connect operators.
     */
    private void connectOperators() throws PlanGenException {
        HashSet<String> unvisitedVertices = new HashSet<>(adjacencyList.keySet());
        HashSet<String> visitedVertices = new HashSet<>();
        
        for (String vertex : adjacencyList.keySet()) {
            if (unvisitedVertices.contains(vertex)) {
                connectOperatorDfsVisit(vertex, unvisitedVertices, visitedVertices);
            }
        }
    }
    
    /*
     * This is a helper function to traverse the graph using DFS algorithm for connecting operators.
     */
    private void connectOperatorDfsVisit(String vertex, HashSet<String> unvisitedVertices, HashSet<String> visitedVertices) throws PlanGenException {
        unvisitedVertices.remove(vertex);
        IOperator currentOperator = operatorObjectMap.get(vertex);
        int outputArity = adjacencyList.get(vertex).size();
        
        // automatically adds a OneToNBroadcastConnector if the output arity > 1.s
        if (outputArity > 1) {
            OneToNBroadcastConnector oneToNConnector = new OneToNBroadcastConnector(outputArity);
            oneToNConnector.setInputOperator(currentOperator);
            int counter = 0;
            for (String adjacentVertex : adjacencyList.get(vertex)) {
                IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
                handleSetInputOperator(oneToNConnector.getOutputOperator(counter), adjacentOperator);
                
                if (unvisitedVertices.contains(adjacentVertex)) {
                    connectOperatorDfsVisit(adjacentVertex, unvisitedVertices, visitedVertices);
                }
            }
        } else {
            for (String adjacentVertex : adjacencyList.get(vertex)) {
                IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
                handleSetInputOperator(currentOperator, adjacentOperator);

                if (unvisitedVertices.contains(adjacentVertex)) {
                    connectOperatorDfsVisit(adjacentVertex, unvisitedVertices, visitedVertices);
                }
            }
        }
        
        visitedVertices.add(vertex);      
    }
    
    private void handleSetInputOperator(IOperator from, IOperator to) throws PlanGenException {
        // handles Join operator differently
        if (to instanceof Join) {
            Join join = (Join) to;
            if (! joinOperatorLinkMap.containsKey(join)) {
                joinOperatorLinkMap.put(join, new ArrayList<>());
            }
            ArrayList<Object> joinOperatorInputs = joinOperatorLinkMap.get(join);
            if (joinOperatorInputs.size() == 0) {
                join.setInnerInputOperator(from);
            } else {
                join.setOuterInputOperator(from);
            }
            joinOperatorInputs.add(from);
        // invokes "setInputOperator" for all other operators
        } else {
            try {
                to.getClass().getMethod("setInputOperator", IOperator.class).invoke(to, from);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException 
                    | IllegalArgumentException | InvocationTargetException e) {
                throw new PlanGenException(e.getMessage(), e);
            }  
        }
    }
     
    /*
     * Finds the sink operator in the operator graph.
     * 
     * This functions assumes that the graph is valid and there is only one sink in the graph.
     */
    private ISink findSinkOperator() throws PlanGenException {
        IOperator sinkOperator = adjacencyList.keySet().stream()
                .filter(operator -> operatorTypeMap.get(operator).toLowerCase().contains("sink"))
                .map(operator -> operatorObjectMap.get(operator))
                .findFirst().orElse(null);
        
        PlanGenUtils.planGenAssert(sinkOperator != null, "Error: sink operator doesn't exist.");
        PlanGenUtils.planGenAssert(sinkOperator instanceof ISink, "Error: sink operator's type doesn't match.");
        
        return (ISink) sinkOperator;
    }
 
}
