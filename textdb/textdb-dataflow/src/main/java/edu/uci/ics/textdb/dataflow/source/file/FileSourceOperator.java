package edu.uci.ics.textdb.dataflow.source.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.Scanner;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
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
    private Schema outputSchema;
    
    private int fileListCursor = 0;
    private boolean isOpen = false;
    
    public FileSourceOperator(FileSourcePredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public void open() throws TextDBException {
        if(isOpen) {
            return;
        }
        isOpen = true;
        outputSchema = new Schema(new Attribute(predicate.getFieldName(), predicate.getFieldType()));
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (! isOpen) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        if (fileListCursor >= predicate.getFilePathList().size()) {
            return null;
        }
        try {
            String filePath = predicate.getFilePathList().get(fileListCursor);
            Scanner scanner = new Scanner(new File(filePath));
            StringBuilder sb = new StringBuilder();
            while (scanner.hasNextLine()) {
                sb.append(scanner.nextLine());
            }
            scanner.close();
            return new Tuple(outputSchema, 
                    Utils.getField(predicate.getFieldType(), sb.toString()));
        } catch (ParseException | FileNotFoundException e) {
            throw new DataFlowException(e);
        }
    }

    @Override
    public void close() throws TextDBException {
        isOpen = false;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

}
