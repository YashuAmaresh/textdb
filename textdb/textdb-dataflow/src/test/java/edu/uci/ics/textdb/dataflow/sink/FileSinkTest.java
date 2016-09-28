package edu.uci.ics.textdb.dataflow.sink;

import java.io.File;
import java.io.FileNotFoundException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;

public class FileSinkTest {

    private FileSink fileSink;
    private IOperator childOperator;
    private File file;

    @Before
    public void setUp() throws FileNotFoundException {
        childOperator = Mockito.mock(IOperator.class);
        file = new File("sample.txt");
        fileSink = new FileSink(file);
        fileSink.setInputOperator(childOperator);
    }

    @After
    public void cleanUp() {
        if (file != null) {
            file.delete();
        }
    }

    @Test
    public void testOpen() throws Exception {
        fileSink.open();
        // verify that childOperator called open() method
        Mockito.verify(childOperator).open();
    }

    @Test
    public void testClose() throws Exception {
        fileSink.close();
        // verify that childOperator called close() method
        Mockito.verify(childOperator).close();
    }

    @Test
    public void testProcessTuples() throws Exception {
        ITuple sampleTuple = Mockito.mock(ITuple.class);
        Mockito.when(sampleTuple.toString()).thenReturn("Sample Tuple");
        // Set the behavior for childOperator,
        // first it returns some non-null tuple and second time it returns null
        Mockito.when(childOperator.getNextTuple()).thenReturn(sampleTuple).thenReturn(null);
        fileSink.open();
        fileSink.processTuples();
        // Verify that childOperator.getNextTuple() is called twice
        Mockito.verify(childOperator, Mockito.times(2)).getNextTuple();
        fileSink.close();

    }
}
