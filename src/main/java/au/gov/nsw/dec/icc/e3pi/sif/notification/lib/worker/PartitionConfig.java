package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

class PartitionConfig {
    private final int maxRecords;
    private final long idleTimeInMilliseconds;
    private final boolean callProcessRecordsEvenForEmptyRecordList;


    public PartitionConfig(int maxRecords,
                           long idleTimeInMilliseconds,
                           boolean callProcessRecordsEvenForEmptyRecordList) {
        this.maxRecords = maxRecords;
        this.idleTimeInMilliseconds = idleTimeInMilliseconds;
        this.callProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList;
    }

    int getMaxRecords() {
        return maxRecords;
    }

    long getIdleTimeInMilliseconds() {
        return idleTimeInMilliseconds;
    }

    boolean isCallProcessRecordsEvenForEmptyRecordList() {
        return callProcessRecordsEvenForEmptyRecordList;
    }
}
