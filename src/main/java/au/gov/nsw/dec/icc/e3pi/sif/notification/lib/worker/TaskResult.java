package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

class TaskResult {
    private Exception exception;

    public TaskResult(Exception exception) {
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }
}
