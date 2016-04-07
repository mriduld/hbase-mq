package au.gov.nsw.dec.icc.e3pi.sif.notification;

import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordCheckPointer;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordProcessor;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordProcessorFactory;
import au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker.ApplicationConfig;
import au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker.Worker;
import au.gov.nsw.dec.icc.e3pi.sif.notification.model.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestApp {
    private static final String ZOOKEEPER_QUORUM = "hadoop-vm";
    private static final String ZOOKEEPER_CLIENT_PORT = "2181";


    private static Configuration getConfiguration(){
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, ZOOKEEPER_QUORUM);
        return conf;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ExecutorService es = Executors.newSingleThreadExecutor();
        Worker worker = new Worker(
                getApplicationConfig(),
                getIRecordProcessorFactory(),
                Arrays.asList(
                        "sif:obj.roominfo",
                        "sif:obj.scheduledactivity",
                        "sif:obj.schoolcourseinfo",
                        "sif:obj.schoolinfo",
                        "sif:obj.staffassignment",
                        "sif:obj.staffpersonal",
                        "sif:obj.studentattendancetimelist",
                        "sif:obj.studentcontactpersonal",
                        "sif:obj.studentcontactrelationship",
                        "sif:obj.studentpersonal",
                        "sif:obj.studentschoolenrollment",
                        "sif:obj.teachinggroup",
                        "sif:obj.timetable",
                        "sif:obj.timetablecell",
                        "sif:obj.timetablesubject"),
                getConfiguration());

        es.submit(worker);
        long seconds = TimeUnit.MINUTES.toSeconds(30) + new Random().nextInt(50);
        System.out.println("Time => " + seconds);
        TimeUnit.SECONDS.sleep(seconds);
        worker.shutdown();
        while (!worker.isShutdown()){
            Thread.sleep(100);
        }
        es.shutdownNow();
    }

    private static ApplicationConfig getApplicationConfig() {
        return ApplicationConfig.builder("test")
                .withCallProcessRecordsEvenForEmptyRecordList(true)
                .withIdleTimeBetweenReadsMillis(5000)
                .withMaxRecords(5000)
                .build();
    }

    private static IRecordProcessorFactory getIRecordProcessorFactory() {
        return new IRecordProcessorFactory() {
            @Override
            public IRecordProcessor create() {
                return new RecordsProcessor();
            }
        };
    }

    private static class RecordsProcessor implements IRecordProcessor{
        private static final Log LOG = LogFactory.getLog(RecordsProcessor.class);

        private final AtomicInteger count = new AtomicInteger();
        private String partition;

        @Override
        public void initialize(IPartition partition) {
            this.partition = partition.getId();
        }

        @Override
        public void processRecords(List<Record> records, IRecordCheckPointer checkPointer) throws Exception {
            int size = records.size();
            checkPointer.checkPoint();
            LOG.info(String.format("[%s] processed %d records", partition, count.addAndGet(size)));
        }

        @Override
        public void shutdown(IRecordCheckPointer checkpointer) throws Exception {
            LOG.info(String.format("[%s] shutting down", partition));
            checkpointer.checkPoint();
        }
    }
}
