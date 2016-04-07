package au.gov.nsw.dec.icc.e3pi.sif.notification.model;

import java.util.Map;

public class Record {
    private final byte[] id;
    private final Map<byte[],byte[]> data;

    public Record(byte[] id, Map<byte[], byte[]> data) {
        this.id = id;
        this.data = data;
    }

    public byte[] getId() {
        return id;
    }

    public Map<byte[], byte[]> getData() {
        return data;
    }
}
