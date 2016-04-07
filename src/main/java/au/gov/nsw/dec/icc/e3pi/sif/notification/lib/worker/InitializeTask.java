package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.ICheckpoint;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordProcessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class InitializeTask implements ITask {
    private static final Log LOG = LogFactory.getLog(InitializeTask.class);

    private final IPartition partition;
    private final IRecordProcessor recordProcessor;
    private final ICheckpoint checkpoint;
    private final HBaseRecordFetcher recordFetcher;
    private final RecordProcessorCheckPointer checkPointer;
    private final long backoffTimeMillis;

    InitializeTask(IPartition partition,
                   IRecordProcessor recordProcessor,
                   ICheckpoint checkpoint,
                   HBaseRecordFetcher recordFetcher,
                   RecordProcessorCheckPointer checkPointer,
                   long backoffTimeMillis) {
        this.partition = partition;
        this.recordProcessor = recordProcessor;
        this.checkpoint = checkpoint;
        this.recordFetcher = recordFetcher;
        this.checkPointer = checkPointer;
        this.backoffTimeMillis = backoffTimeMillis;
    }

    @Override
    public TaskResult call() throws Exception {
        Exception exception = null;
        LOG.info("Initializing partition " + partition);
        try {
            byte[] checkPoint = checkpoint.getCheckPoint(partition);
            checkPointer.setLargestPermittedCheckpointValue(checkPoint);
            checkPointer.setLastCheckPoint(checkPoint);
            recordFetcher.initialize(checkPoint);
            recordProcessor.initialize(partition);
            LOG.info("RecordProcessor Initialization Completed for partition "+ partition);
        } catch (Exception e) {
            exception = e;
            LOG.error("Error initializing task ", e);
            Thread.sleep(backoffTimeMillis);
        }
        return new TaskResult(exception);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.INITIALIZE;
    }
}
