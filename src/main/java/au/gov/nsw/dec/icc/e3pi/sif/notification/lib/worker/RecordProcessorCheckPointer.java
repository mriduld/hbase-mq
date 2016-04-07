package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.ICheckpoint;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordCheckPointer;

class RecordProcessorCheckPointer implements IRecordCheckPointer {
    private final ICheckpoint checkpoint;
    private final IPartition partition;

    private byte[] largestPermittedCheckpointValue;
    private byte[] lastCheckPoint;

    RecordProcessorCheckPointer(ICheckpoint checkpoint, IPartition partition) {
        this.checkpoint = checkpoint;
        this.partition = partition;
    }

    @Override
    public void checkPoint() throws Exception {
        checkpoint.setCheckPoint(partition, largestPermittedCheckpointValue);
    }

    synchronized void setLargestPermittedCheckpointValue(byte[] largestPermittedCheckpointValue) {
        this.largestPermittedCheckpointValue = largestPermittedCheckpointValue;
    }

    synchronized void setLastCheckPoint(byte[] lastCheckPoint) {
        this.lastCheckPoint = lastCheckPoint;
    }
}
