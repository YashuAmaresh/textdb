package edu.uci.ics.textdb.dataflow.source.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.utils.Utils;

/**
 * FileSourceOperator treats files on disk as a source. FileSourceOperator reads
 * a file line by line. A user needs to provide a custom function to convert a
 * string to tuple.
 * 
 * @author zuozhi
 */
public class FileSourceOperator implements ISourceOperator {
    
    private FileSourcePredicate predicate;
    private Scanner scanner;
    private boolean isFinished;
    
    public FileSourceOperator(FileSourcePredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public void open() throws TextDBException {
        try {
            scanner = new Scanner(new File(predicate.getFilePath()));
            isFinished = false;
        } catch (FileNotFoundException e) {
            throw new TextDBException("Failed to open FileSourceOperator\n" + e.getMessage(), e);
        }
    }

    @Override
    public ITuple getNextTuple() throws TextDBException {
        if (isFinished) {
            return null;
        }
        isFinished = true;
        StringBuilder sb = new StringBuilder();
        while (scanner.hasNextLine()) {
            sb.append(scanner.nextLine());
        }
        return new DataTuple(new Attribute(predicate.getFieldName(), predicate.getFieldType()), 
                Utils.getField(predicate.getFieldType(), sb.toString()));
    }

    @Override
    public void close() throws TextDBException {
        if (this.scanner != null) {
            this.scanner.close();
        }
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

}
