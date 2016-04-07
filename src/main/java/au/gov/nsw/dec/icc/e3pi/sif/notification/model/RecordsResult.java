package au.gov.nsw.dec.icc.e3pi.sif.notification.model;

import java.util.List;

public class RecordsResult {
    private final List<Record> records;
    private final byte[] lastIterator;

    public RecordsResult(List<Record> records, byte[] lastIterator) {
        this.records = records;
        this.lastIterator = lastIterator;
    }

    public List<Record> getRecords() {
        return records;
    }

    public byte[] getLastIterator() {
        return lastIterator;
    }
}
