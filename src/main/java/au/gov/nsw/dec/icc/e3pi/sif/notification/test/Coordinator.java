package au.gov.nsw.dec.icc.e3pi.sif.notification.test;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.dgc.VMID;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Coordinator implements Closeable {
    private static final Log LOG = LogFactory.getLog(Coordinator.class);

    private final String PARTICIPANT_PATH = "/participants";
    private final String LEADER_PATH = "/latch";

    private final List<String> partitions;
    private final CuratorFramework curator;
    private final PathChildrenCache cache;
    private final Map<String, LeaderLatch> map;
    private final Set<String> owned;
    private final String id;

    public Coordinator(List<String> partitions, CuratorFramework curator) {
        this.partitions = partitions;
        this.curator = curator;
        this.cache = new PathChildrenCache(curator, PARTICIPANT_PATH, false);
        this.map = new ConcurrentHashMap<>();
        this.owned = new HashSet<>();
        this.id = new VMID().toString();
    }

    void start() throws Exception {
        cache.getListenable().addListener(new CacheListener());
        cache.start();
        curator.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(PARTICIPANT_PATH + "/" + id, new byte[0]);
    }

    private class CacheListener implements PathChildrenCacheListener {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            switch (event.getType()) {
                case CHILD_ADDED:
                    LOG.info("Child Added");
                    redistribute();
                    break;
                case CHILD_REMOVED:
                    LOG.info("Child Removed");
                    redistribute();
                    break;
            }
        }
    }

    private synchronized void redistribute() {
        List<String> participants;
        try {
            participants = curator.getChildren().forPath(PARTICIPANT_PATH);
            LOG.info("Participants => " + participants);
        } catch (Exception e) {
            participants = Collections.singletonList(id);
            LOG.error("Error retrieving participants");
        }

        int maxPartitions = (int) Math.ceil((double) partitions.size() / participants.size());
        if (owned.size() != maxPartitions) {
            if (owned.size() < maxPartitions) {
                LOG.info(String.format("max partitions: %d. I have %d. Need to acquire", maxPartitions, owned.size()));
                for (String partition : partitions) {
                    if (!owned.contains(partition)) {
                        String path = LEADER_PATH + "/" + partition;
                        boolean acquire = true;
                        try {
                            acquire = curator.checkExists().forPath(path) == null ||
                                    curator.getChildren().forPath(path).isEmpty();
                        } catch (Exception e) {
                            LOG.error("Error retrieving children");
                        }
                        if (acquire) {
                            LOG.info("Can acquire => " + partition);
                            LeaderLatch latch = new LeaderLatch(curator, path, id, LeaderLatch.CloseMode.NOTIFY_LEADER);
                            latch.addListener(new LeaderListener(partition));
                            try {
                                latch.start();
                                map.put(partition, latch);
                            } catch (Exception e) {
                                LOG.error("Error starting leader latch for path " + path);
                                LOG.error(e.getLocalizedMessage(), e);
                                CloseableUtils.closeQuietly(latch);
                            }
                        }
                    } else {
                        LOG.info(String.format("Already own %s", partition));
                    }
                    if (owned.size() >= maxPartitions) {
                        break;
                    }
                }
            } else if (owned.size() > maxPartitions) {
                LOG.info(String.format("max partitions: %d. I have %d. Need to give up ", maxPartitions, owned.size()));
                List<String> ownedPartitions = Lists.newArrayList(owned);
                Collections.shuffle(ownedPartitions);
                for (String partition : ownedPartitions) {
                    try {
                        map.get(partition).close();
                    } catch (IOException e) {
                        LOG.error(e.getLocalizedMessage(), e);
                    }
                    if (owned.size() <= maxPartitions) {
                        break;
                    }
                }
            }
        } else {
            LOG.info("I have maximum partitions ");
        }
    }

    @Override
    public void close() throws IOException {
        cache.close();
        for (LeaderLatch latch : map.values()) {
            latch.close();
        }
        owned.clear();
    }

    Set<String> getOwned() {
        return owned;
    }

    private class LeaderListener implements LeaderLatchListener {
        private final String partition;

        LeaderListener(String partition) {
            this.partition = partition;
        }

        @Override
        public void isLeader() {
            owned.add(partition);
        }

        @Override
        public void notLeader() {
            owned.remove(partition);
            map.remove(partition);
        }
    }

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("test")
                .ensembleProvider(new FixedEnsembleProvider("hadoop-vm:2181"))
                .build();
        client.start();
        Coordinator coordinator = null;
        try {
            client.getZookeeperClient().blockUntilConnectedOrTimedOut();
            coordinator = new Coordinator(Arrays.asList("ONE", "TWO", "THREE"), client);
            coordinator.start();
            int seconds = 20 + new Random().nextInt(20);
            LOG.info("Times => " + seconds);
            for (int i = 0; i < seconds; i++) {
                LOG.info("Currently owned => " + coordinator.getOwned());
                TimeUnit.SECONDS.sleep(5);
            }
        } finally {
            CloseableUtils.closeQuietly(coordinator);
            CloseableUtils.closeQuietly(client);
        }
    }
}
