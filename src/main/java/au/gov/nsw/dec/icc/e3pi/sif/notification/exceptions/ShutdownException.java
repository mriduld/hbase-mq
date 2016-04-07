package au.gov.nsw.dec.icc.e3pi.sif.notification.exceptions;


public class ShutdownException extends Exception {
    public ShutdownException(String message) {
        super(message);
    }

    public ShutdownException(String message, Throwable cause) {
        super(message, cause);
    }
}
