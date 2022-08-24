package org.github.ogomezso.wallclocktransformer;

public class UnsupportedOperation extends RuntimeException {
    public UnsupportedOperation() {
    }

    public UnsupportedOperation(String message) {
        super(message);
    }

    public UnsupportedOperation(String message, Throwable cause) {
        super(message, cause);
    }
}
