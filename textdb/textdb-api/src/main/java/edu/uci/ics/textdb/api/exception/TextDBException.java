package edu.uci.ics.textdb.api.exception;

/**
 * Superclass of all exceptions inside TextDB Engine.
 */
public class TextDBException extends RuntimeException {

    private static final long serialVersionUID = 4359106470500687632L;

    public TextDBException(String errorMessage) {
        super(errorMessage);
    }
    
    public TextDBException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }
    
    public TextDBException(Throwable throwable) {
        super(throwable);
    }
    
}
