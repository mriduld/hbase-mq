package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCurator {
    private CuratorFramework client;

    @Before
    public void setUp() throws InterruptedException {
        client = CuratorFrameworkFactory.builder()
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("test")
                .ensembleProvider(new FixedEnsembleProvider("hadoop-vm:2181"))
                .build();
        client.start();
        client.blockUntilConnected();
    }

    @After
    public void tearDown(){
        client.close();
    }

    @Test
    public void testPath() throws Exception {
        client.createContainers("/leader"+"/" +"one");
    }
}
