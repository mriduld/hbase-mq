package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.exceptions.ShutdownException;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordProcessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class ShuttingDownTask implements ITask {
    private static final Log LOG = LogFactory.getLog(ShuttingDownTask.class);

    private final IPartition partition;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckPointer recordCheckPointer;
    private final long backoffTimeMillis;

    ShuttingDownTask(IPartition partition,
                     IRecordProcessor recordProcessor,
                     RecordProcessorCheckPointer recordCheckPointer,
                     long backoffTimeMillis) {
        this.partition = partition;
        this.recordProcessor = recordProcessor;
        this.recordCheckPointer = recordCheckPointer;
        this.backoffTimeMillis = backoffTimeMillis;
    }

    @Override
    public TaskResult call() throws Exception {
        Exception exception = null;
        LOG.debug("Invoking shutdown() for partition " + partition.getId());
        try {
            recordProcessor.shutdown(recordCheckPointer);
            LOG.debug("Record processor completed shutdown() for partition " + partition.getId());
        } catch (ShutdownException se){
            LOG.error(se.getLocalizedMessage());
        } catch (Exception e) {
            exception = e;
            try {
                Thread.sleep(this.backoffTimeMillis);
            } catch (InterruptedException ie) {
                LOG.debug("Interrupted sleep", ie);
            }
        }
        return new TaskResult(exception);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.SHUT_DOWN;
    }
}
