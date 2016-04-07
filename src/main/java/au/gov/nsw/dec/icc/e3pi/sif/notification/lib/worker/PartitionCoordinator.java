package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.exceptions.ShutdownException;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.ICheckpoint;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.rmi.dgc.VMID;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class PartitionCoordinator implements ICheckpoint {
    private static final Log LOG = LogFactory.getLog(PartitionCoordinator.class);

    private final String PARTICIPANT_PATH = "/participants";
    private final String LEADER_PATH = "/owner";
    private final String HWM_PATH = "/hwm";

    private final List<IPartition> partitions;
    private final CuratorFramework curator;
    private final PathChildrenCache cache;
    private final Map<IPartition, LeaderLatch> map;
    private final Set<IPartition> owned;
    private final String id;

    PartitionCoordinator(List<IPartition> partitions, CuratorFramework curator) {
        this.partitions = partitions;
        this.curator = curator;
        this.cache = new PathChildrenCache(curator, PARTICIPANT_PATH, false);
        this.map = new ConcurrentHashMap<>();
        this.owned = new HashSet<>();
        this.id = new VMID().toString();
    }

    void initialize() throws Exception {
        cache.getListenable().addListener(new CacheListener());
        cache.start();
        curator.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(PARTICIPANT_PATH + "/" + id, new byte[0]);

        for (IPartition partition : partitions) {
            curator.createContainers(getHwmPath(partition));
        }
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
                for (IPartition partition : partitions) {
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
                List<IPartition> ownedPartitions = Lists.newArrayList(owned);
                Collections.shuffle(ownedPartitions);
                for (IPartition partition : ownedPartitions) {
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

    void close()  {
        CloseableUtils.closeQuietly(cache);
        for (LeaderLatch latch : map.values()) {
            CloseableUtils.closeQuietly(latch);
        }
        owned.clear();
    }

    List<IPartition> getAssignedPartitions() {
        return ImmutableList.copyOf(owned);
    }

    @Override
    public synchronized void setCheckPoint(IPartition partition, byte[] checkPoint) throws Exception {
        if(!owned.contains(partition)){
            throw new ShutdownException("Can't update checkpoint - instance doesn't own this partition");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting Checkpoint at " + Bytes.toStringBinary(checkPoint));
        }
        curator.setData().forPath(getHwmPath(partition), checkPoint);
    }

    @Override
    public byte[] getCheckPoint(IPartition partition) throws Exception {
        return curator.getData().forPath(getHwmPath(partition));
    }


    private String getHwmPath(IPartition partition) {
        return HWM_PATH + "/" + partition.getId();
    }

    private class LeaderListener implements LeaderLatchListener {
        private final IPartition partition;

        LeaderListener(IPartition partition) {
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
}
