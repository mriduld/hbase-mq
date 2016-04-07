package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.exceptions.ShutdownException;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordProcessor;
import au.gov.nsw.dec.icc.e3pi.sif.notification.model.Record;
import au.gov.nsw.dec.icc.e3pi.sif.notification.model.RecordsResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

class ProcessTask implements ITask {
    private static final Log LOG = LogFactory.getLog(ProcessTask.class);

    private final IPartition partition;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckPointer checkPointer;
    private final HBaseRecordFetcher recordFetcher;
    private final PartitionConfig config;
    private final long backoffTimeMillis;

    ProcessTask(IPartition partition,
                IRecordProcessor recordProcessor,
                RecordProcessorCheckPointer checkPointer,
                HBaseRecordFetcher recordFetcher,
                PartitionConfig config,
                long backoffTimeMillis) {
        this.partition = partition;
        this.recordProcessor = recordProcessor;
        this.checkPointer = checkPointer;
        this.recordFetcher = recordFetcher;
        this.config = config;
        this.backoffTimeMillis = backoffTimeMillis;
    }

    @Override
    public TaskResult call() throws Exception {
        Exception exception = null;

        try {
            RecordsResult recordsResult = recordFetcher.getRecords(config.getMaxRecords());
            List<Record> records = recordsResult.getRecords();
            if (records.isEmpty()) {
                LOG.debug("HBase didn't return any records for shard " + partition.getId());
                try {
                    LOG.debug("Sleeping for " + config.getIdleTimeInMilliseconds() + " ms since there were no new records in partition "
                            + partition.getId());
                    Thread.sleep(config.getIdleTimeInMilliseconds());
                } catch (InterruptedException e) {
                    LOG.debug("ShardId " + partition.getId() + ": Sleep was interrupted");
                }
            }
            if(!records.isEmpty() || config.isCallProcessRecordsEvenForEmptyRecordList()){
                try {
                    checkPointer.setLargestPermittedCheckpointValue(recordsResult.getLastIterator());
                    recordProcessor.processRecords(records, checkPointer);
                } catch (ShutdownException se){
                    LOG.error(partition + " was shutdown while processing.");
                } catch (Exception e) {
                    LOG.error("Partition " + partition.getId()
                            + ": Application processRecords() threw an exception when processing shard ", e);
                    LOG.error("Partition " + partition.getId() + ": Skipping over the following data records: "
                            + records);
                }
            }
        } catch (Exception ex) {
            exception = ex;
            LOG.error("Application exception. ", ex);
            try {
                Thread.sleep(backoffTimeMillis);
            } catch (InterruptedException ie) {
                LOG.debug("Interrupted sleep", ie);
            }
        }
        return new TaskResult(exception);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.PROCESS;
    }
}
