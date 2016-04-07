package au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces;

public interface ICheckpoint {
    void setCheckPoint(IPartition partition, byte[] checkPoint) throws Exception;

    byte[] getCheckPoint(IPartition partition) throws Exception;
}
