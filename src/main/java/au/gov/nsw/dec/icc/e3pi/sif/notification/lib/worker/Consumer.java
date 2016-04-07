package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.ICheckpoint;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordProcessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Connection;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

class Consumer {
    private static final Log LOG = LogFactory.getLog(Consumer.class);

    private enum ConsumerState {
        INITIALIZING, PROCESSING, SHUTTING_DOWN, SHUTDOWN_COMPLETE
    }

    private final IPartition partition;
    private final PartitionConfig partitionConfig;
    private final IRecordProcessor recordProcessor;
    private final ICheckpoint checkpoint;
    private final RecordProcessorCheckPointer recordProcessorCheckpointer;
    private final HBaseRecordFetcher dataFetcher;
    private final ExecutorService es;
    private final long taskBackoffTimeMillis;


    private Future<TaskResult> future;

    private ConsumerState currentState = ConsumerState.INITIALIZING;

    private ITask currentTask;
    private long currentTaskSubmitTime;

    private volatile boolean beginShutdown;

    Consumer(IPartition partition,
             PartitionConfig partitionConfig,
             IRecordProcessor recordProcessor,
             ICheckpoint checkpoint,
             Connection connection,
             ExecutorService es,
             long taskBackoffTimeMillis) {
        this.partition = partition;
        this.partitionConfig = partitionConfig;
        this.recordProcessor = recordProcessor;
        this.checkpoint = checkpoint;
        this.recordProcessorCheckpointer = new RecordProcessorCheckPointer(checkpoint, partition);
        this.dataFetcher = new HBaseRecordFetcher(partition, connection, es);
        this.es = es;
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
    }

    boolean consume() {
        return checkAndSubmitNextTask();
    }

    private synchronized boolean checkAndSubmitNextTask() {
        boolean taskCompleted = false;
        boolean submittedNewTask = false;
        if (future == null || future.isDone() || future.isCancelled()) {
            if (future != null && future.isDone()) {
                try {
                    TaskResult result = future.get();
                    if(result.getException() == null){
                        taskCompleted = true;
                    } else {
                        Exception taskException = result.getException();
                        LOG.error("Caught exception running " + currentTask.getTaskType() + " task: ", taskException);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    future = null;
                }
            }
            updateState(taskCompleted);
            ITask nextTask = getNextTask();
            if (nextTask != null) {
                currentTask = nextTask;
                try {
                    future = es.submit(currentTask);
                    currentTaskSubmitTime = System.currentTimeMillis();
                    submittedNewTask = true;
                    if(LOG.isDebugEnabled()){
                        LOG.info("Submitted new " + currentTask.getTaskType()
                                + " task for shard " + partition.getId());
                    }
                } catch (RejectedExecutionException e) {
                    LOG.info(currentTask.getTaskType() + " task was not accepted for execution.", e);
                } catch (RuntimeException e) {
                    LOG.info(currentTask.getTaskType() + " task encountered exception ", e);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("No new task to submit for partition %s, currentState %s",
                            partition.getId(),
                            currentState.toString()));
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Previous " + currentTask.getTaskType() + " task still pending for partition "
                        + partition.getId() + " since " + (System.currentTimeMillis() - currentTaskSubmitTime)
                        + " ms ago" + ".  Not submitting new task.");
            }
        }
        return submittedNewTask;
    }

    private ITask getNextTask() {
        ITask nextTask = null;
        switch (currentState) {
            case INITIALIZING:
                nextTask = new InitializeTask(partition,
                        recordProcessor,
                        checkpoint,
                        dataFetcher,
                        recordProcessorCheckpointer,
                        taskBackoffTimeMillis
                );
                break;
            case PROCESSING:
                nextTask = new ProcessTask(partition,
                                           recordProcessor,
                                           recordProcessorCheckpointer,
                                           dataFetcher,
                                           partitionConfig,
                                           taskBackoffTimeMillis
                                           );
                break;
            case SHUTTING_DOWN:
                nextTask = new ShuttingDownTask(partition,
                                               recordProcessor,
                                               recordProcessorCheckpointer,
                                               taskBackoffTimeMillis);
                break;
            case SHUTDOWN_COMPLETE:
                break;
        }
        return nextTask;
    }

    private void updateState(boolean taskCompleted) {
        switch (currentState) {
            case INITIALIZING:
                if (taskCompleted && TaskType.INITIALIZE == currentTask.getTaskType()) {
                    if (beginShutdown) {
                        currentState = ConsumerState.SHUTTING_DOWN;
                    } else {
                        currentState = ConsumerState.PROCESSING;
                    }
                } else if (currentTask == null && beginShutdown) {
                    currentState = ConsumerState.SHUTDOWN_COMPLETE;
                }
                break;
            case PROCESSING:
                if (taskCompleted && TaskType.PROCESS == currentTask.getTaskType()) {
                    if (beginShutdown) {
                        currentState = ConsumerState.SHUTTING_DOWN;
                    } else {
                        currentState = ConsumerState.PROCESSING;
                    }
                } else if (currentTask == null && beginShutdown) {
                    currentState = ConsumerState.SHUTDOWN_COMPLETE;
                }
                break;
            case SHUTTING_DOWN:
                if (currentTask == null || (taskCompleted && TaskType.SHUT_DOWN == currentTask.getTaskType())) {
                    currentState = ConsumerState.SHUTDOWN_COMPLETE;
                }
                break;
            case SHUTDOWN_COMPLETE:
                break;
            default:
                LOG.error("Unexpected state: " + currentState);
        }
    }

    synchronized boolean beginShutDown() {
        if (currentState != ConsumerState.SHUTDOWN_COMPLETE) {
            markForShutdown();
            checkAndSubmitNextTask();
        }
        return isShutdown();
    }

    synchronized void markForShutdown() {
        beginShutdown = true;
    }

    boolean isShutdown() {
        return currentState == ConsumerState.SHUTDOWN_COMPLETE;
    }

    public ConsumerState getCurrentState() {
        return currentState;
    }
}
