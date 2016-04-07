package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

public class ApplicationConfig {

    static final long DEFAULT_FAILOVER_TIME_MILLIS = 10000L;
    static final int DEFAULT_MAX_RECORDS = 500;
    static final long DEFAULT_IDLETIME_BETWEEN_READS_MILLIS = 1000L;
    static final long DEFAULT_TASK_BACKOFF_TIME_MILLIS = 500L;
    static final boolean DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LISTS = false;

    private final String applicationName;
    private final long failOverTimeMillis;
    private final int maxRecords;
    private final boolean callProcessRecordsEvenForEmptyRecordList;
    private final long idleTimeBetweenReadsMillis;
    private final long taskBackoffTimeMillis;

    public ApplicationConfig(String applicationName,
                             long failOverTimeMillis,
                             int maxRecords,
                             boolean callProcessRecordsEvenForEmptyRecordList,
                             long idleTimeBetweenReadsMillis,
                             long taskBackoffTimeMillis) {
        this.applicationName = applicationName;
        this.failOverTimeMillis = failOverTimeMillis;
        this.maxRecords = maxRecords;
        this.callProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList;
        this.idleTimeBetweenReadsMillis = idleTimeBetweenReadsMillis;
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public long getFailOverTimeMillis() {
        return failOverTimeMillis;
    }

    public int getMaxRecords() {
        return maxRecords;
    }

    public long getIdleTimeBetweenReadsMillis() {
        return idleTimeBetweenReadsMillis;
    }

    public long getTaskBackoffTimeMillis() {
        return taskBackoffTimeMillis;
    }

    public boolean isCallProcessRecordsEvenForEmptyRecordList() {
        return callProcessRecordsEvenForEmptyRecordList;
    }

    public static Builder builder(String applicationName) {
        return new Builder(applicationName);
    }

    public static class Builder {
        private final String applicationName;
        private long failOverTimeMillis = DEFAULT_FAILOVER_TIME_MILLIS;
        private int maxRecords = DEFAULT_MAX_RECORDS;
        private boolean callProcessRecordsEvenForEmptyRecordList = DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LISTS;
        private long idleTimeBetweenReadsMillis = DEFAULT_IDLETIME_BETWEEN_READS_MILLIS;
        private long taskBackoffTimeMillis = DEFAULT_TASK_BACKOFF_TIME_MILLIS;

        public Builder(String applicationName) {
            this.applicationName = applicationName;
        }

        public Builder withFailOverTimeMillis(long failOverTimeMillis) {
            this.failOverTimeMillis = failOverTimeMillis;
            return this;
        }

        public Builder withMaxRecords(int maxRecords) {
            this.maxRecords = maxRecords;
            return this;
        }

        public Builder withCallProcessRecordsEvenForEmptyRecordList(boolean callProcessRecordsEvenForEmptyRecordList) {
            this.callProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList;
            return this;
        }

        public Builder withIdleTimeBetweenReadsMillis(long idleTimeBetweenReadsMillis) {
            this.idleTimeBetweenReadsMillis = idleTimeBetweenReadsMillis;
            return this;
        }

        public Builder withTaskBackoffTimeMillis(long taskBackoffTimeMillis) {
            this.taskBackoffTimeMillis = taskBackoffTimeMillis;
            return this;
        }

        public ApplicationConfig build() {
            return new ApplicationConfig(
                    applicationName,
                    failOverTimeMillis,
                    maxRecords,
                    callProcessRecordsEvenForEmptyRecordList,
                    idleTimeBetweenReadsMillis,
                    taskBackoffTimeMillis);
        }
    }
}
