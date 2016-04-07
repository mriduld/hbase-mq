package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static org.apache.hadoop.hbase.HConstants.*;

public class Worker implements Runnable {
    private static final Log LOG = LogFactory.getLog(Worker.class);

    private static final int MAX_INITIALIZATION_ATTEMPTS = 20;

    private final String applicationName;
    private final PartitionConfig partitionConfig;
    private final PartitionCoordinator partitionCoordinator;
    private final ICheckpoint checkpoint;
    private final IRecordProcessorFactory recordProcessorFactory;
    private final ExecutorService executorService;
    private final ConcurrentMap<IPartition, Consumer> consumerMap;
    private final long failOverTimeMillis;
    private final long idleTimeInMilliseconds;
    private final long taskBackoffTimeMillis;
    private final Connection connection;
    private final CuratorFramework curator;

    private volatile boolean shutdown;
    private volatile long shutdownStartTimeMillis;

    public Worker(ApplicationConfig appConfig,
                  IRecordProcessorFactory recordProcessorFactory,
                  List<String> partitions,
                  Configuration conf) throws IOException {
        this(appConfig,
                recordProcessorFactory,
                partitions,
                new WorkerThreadPoolExecutor(),
                ConnectionFactory.createConnection(conf),
                getCurator(appConfig.getApplicationName(), conf)
        );
    }

    public Worker(ApplicationConfig appConfig,
                  IRecordProcessorFactory recordProcessorFactory,
                  List<String> partitions,
                  ExecutorService executorService,
                  Connection connection) {
        this(appConfig,
                recordProcessorFactory,
                partitions,
                executorService,
                new WorkerConnection(connection),
                getCurator(appConfig.getApplicationName(), connection.getConfiguration())
        );
    }

    private Worker(ApplicationConfig appConfig,
                   IRecordProcessorFactory recordProcessorFactory,
                   List<String> partitions,
                   ExecutorService executorService,
                   Connection connection,
                   CuratorFramework curator) {
        this(appConfig,
                new PartitionCoordinator(toPartitions(partitions), curator),
                recordProcessorFactory,
                executorService,
                connection,
                curator
        );
    }

    private Worker(ApplicationConfig appConfig,
                   PartitionCoordinator partitionCoordinator,
                   IRecordProcessorFactory recordProcessorFactory,
                   ExecutorService executorService,
                   Connection connection,
                   CuratorFramework curator) {
        this(appConfig.getApplicationName(),
                new PartitionConfig(
                        appConfig.getMaxRecords(),
                        appConfig.getIdleTimeBetweenReadsMillis(),
                        appConfig.isCallProcessRecordsEvenForEmptyRecordList()),
                partitionCoordinator,
                partitionCoordinator,
                recordProcessorFactory,
                executorService,
                connection,
                curator,
                appConfig.getFailOverTimeMillis(),
                appConfig.getIdleTimeBetweenReadsMillis(),
                appConfig.getTaskBackoffTimeMillis()
        );
    }

    private Worker(String applicationName,
                   PartitionConfig partitionConfig,
                   PartitionCoordinator partitionCoordinator,
                   ICheckpoint checkpoint,
                   IRecordProcessorFactory recordProcessorFactory,
                   ExecutorService executorService,
                   Connection connection,
                   CuratorFramework curator,
                   long failOverTimeMillis,
                   long idleTimeInMilliseconds,
                   long taskBackoffTimeMillis
    ) {
        this.applicationName = applicationName;
        this.partitionConfig = partitionConfig;
        this.partitionCoordinator = partitionCoordinator;
        this.checkpoint = checkpoint;
        this.recordProcessorFactory = recordProcessorFactory;
        this.executorService = executorService;
        this.connection = connection;
        this.curator = curator;
        this.failOverTimeMillis = failOverTimeMillis;
        this.idleTimeInMilliseconds = idleTimeInMilliseconds;
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
        this.consumerMap = new ConcurrentHashMap<>();
    }


    @Override
    public void run() {
        if (shutdown) {
            return;
        }
        try {
            initialize();
            LOG.info(applicationName + " Initialization complete. Starting worker loop.");
        } catch (Exception e) {
            LOG.error("Unable to initialize after " + MAX_INITIALIZATION_ATTEMPTS + " attempts. Shutting down.", e);
            shutdown();
        }

        while (!shouldShutDown()) {
            try {
                Set<IPartition> assigned = new HashSet<>();
                for (IPartition partition : getCurrentlyAssignedPartitions()) {
                    Consumer consumer = createOrGetConsumer(partition);
                    consumer.consume();
                    assigned.add(partition);
                }
                cleanUpConsumers(assigned);
                if (LOG.isDebugEnabled()) {
                    LOG.info("Sleeping ... " + idleTimeInMilliseconds);
                }
                Thread.sleep(idleTimeInMilliseconds);
            } catch (Exception e) {
                if (!shutdown) {
                    LOG.error(String.format("Worker.run caught exception, sleeping for %s milli seconds!",
                            String.valueOf(idleTimeInMilliseconds)),
                            e);
                    try {
                        Thread.sleep(idleTimeInMilliseconds);
                    } catch (InterruptedException ex) {
                        LOG.info("Worker: sleep interrupted after catching exception ", ex);
                    }
                }
            }
        }
        finalShutdown();
        LOG.info("Worker loop is complete. Exiting from worker.");
    }

    private List<IPartition> getCurrentlyAssignedPartitions() {
        List<IPartition> partitions = partitionCoordinator.getAssignedPartitions();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Current partitions owned :" + partitions);
        }
        LOG.info("Current Partitions => "+ partitions);
        return partitions;
    }

    private Consumer createOrGetConsumer(IPartition partition) {
        Consumer consumer = consumerMap.get(partition);
        if (consumer == null ||
                consumer.isShutdown()) {
            IRecordProcessor recordProcessor = recordProcessorFactory.create();
            consumer = new Consumer(partition,
                    partitionConfig,
                    recordProcessor,
                    checkpoint,
                    connection,
                    executorService,
                    taskBackoffTimeMillis);
            consumerMap.put(partition, consumer);
            LOG.info("Created new consumer for : " + partition);
        }
        return consumer;
    }

    private void initialize() throws Exception {
        boolean isDone = false;
        Exception lastException = null;
        for (int i = 0; !isDone && i < MAX_INITIALIZATION_ATTEMPTS; i++) {
            try {
                LOG.info("Initialization attempt " + (i + 1));
                curator.getZookeeperClient().blockUntilConnectedOrTimedOut();
                partitionCoordinator.initialize();
                isDone = true;
            } catch (Exception ex) {
                lastException = ex;
                LOG.error(ex.getLocalizedMessage(), ex);
            }
        }
        if (!isDone) {
            throw new RuntimeException(lastException);
        }
    }

    private boolean shouldShutDown() {
        if (executorService.isShutdown()) {
            LOG.error("Worker executor service has been shutdown, so record processors cannot be shutdown.");
            return true;
        }
        if (shutdown) {
            if (consumerMap.isEmpty()) {
                LOG.info("All record processors have been shutdown successfully.");
                return true;
            }
            if ((System.currentTimeMillis() - shutdownStartTimeMillis) >= failOverTimeMillis) {
                LOG.info("Lease failover time is reached, so forcing shutdown.");
                return true;
            }
        }
        return false;
    }

    public void shutdown() {
        LOG.info("Worker shutdown requested.");
        shutdown = true;
        shutdownStartTimeMillis = System.currentTimeMillis();
        partitionCoordinator.close();
    }

    private void finalShutdown() {
        LOG.info("Starting worker's final shutdown.");
        if (executorService instanceof WorkerThreadPoolExecutor) {
            // This should interrupt all active record processor tasks.
            executorService.shutdownNow();
        }
        curator.close();
        CloseableUtils.closeQuietly(connection);
    }

    private void cleanUpConsumers(Set<IPartition> partitions) {
        for (IPartition partition : consumerMap.keySet()) {
            if (!partitions.contains(partition)) {
                boolean isShutDown = consumerMap.get(partition).beginShutDown();
                if (isShutDown) {
                    consumerMap.remove(partition);
                }
            }
        }
    }

    public boolean isShutdown() {
        return consumerMap.isEmpty() || executorService.isTerminated();
    }

    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Extension to ThreadPoolExecutor, so worker can identify whether it owns the executor service instance
     * or not.
     * Visible and non-final only for testing.
     */
    private static class WorkerThreadPoolExecutor extends ThreadPoolExecutor {
        private static final long DEFAULT_KEEP_ALIVE_TIME = 60L;

        WorkerThreadPoolExecutor() {
            // Defaults are based on Executors.newCachedThreadPool()
            super(0, Integer.MAX_VALUE, DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
        }
    }

    private static CuratorFramework getCurator(String namespace, Configuration conf) {
        String zooConnString = String.format("%s:%d", conf.get(ZOOKEEPER_QUORUM), conf.getInt(ZOOKEEPER_CLIENT_PORT, DEFAULT_ZOOKEPER_CLIENT_PORT));
        LOG.info("Zookeeper Connection => " + zooConnString);
        CuratorFramework client =
                CuratorFrameworkFactory.builder()
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace(namespace)
                .ensembleProvider(new FixedEnsembleProvider(zooConnString))
                .build();
        client.start();
        return client;
    }

    private static List<IPartition> toPartitions(List<String> partitionIds) {
        List<IPartition> partitions = new ArrayList<>();
        for (String partitionId : partitionIds) {
            partitions.add(new Partition(partitionId));
        }
        return partitions;
    }
}