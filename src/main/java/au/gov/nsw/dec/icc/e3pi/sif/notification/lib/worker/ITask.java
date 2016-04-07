package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import java.util.concurrent.Callable;

public interface ITask extends Callable<TaskResult> {
    TaskType getTaskType();
}
