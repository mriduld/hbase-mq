package au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces;


import au.gov.nsw.dec.icc.e3pi.sif.notification.model.Record;

import java.util.List;

public interface IRecordProcessor {
    void initialize(IPartition partition);

    void processRecords(List<Record> records, IRecordCheckPointer checkPointer) throws Exception;

    void shutdown(IRecordCheckPointer checkpointer) throws Exception;
}
