package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.ICheckpoint;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordCheckPointer;
import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IRecordProcessor;
import au.gov.nsw.dec.icc.e3pi.sif.notification.model.Record;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class TestConsumer {
    @Test
    public void testConsumer() throws IOException {
        ExecutorService es = MoreExecutors.sameThreadExecutor();
        Partition partition = new Partition("Test");
        IRecordProcessor recordProcessor = new IRecordProcessor() {
            @Override
            public void initialize(IPartition partition) {}

            @Override
            public void processRecords(List<Record> records, IRecordCheckPointer checkPointer) {}

            @Override
            public void shutdown(IRecordCheckPointer checkpointer) {}
        };
        ICheckpoint checkpoint = new ICheckpoint() {
            @Override
            public void setCheckPoint(IPartition partition, byte[] checkpoint) throws Exception {}

            @Override
            public byte[] getCheckPoint(IPartition partition) throws Exception {
                return new byte[0];
            }
        };
        Consumer consumer = new Consumer(
                partition,
                new PartitionConfig(10, 100, true),
                recordProcessor,
                checkpoint,
                getConnection(),
                es,
                100);
        System.out.println(consumer.getCurrentState());
        consumer.consume();
        System.out.println(consumer.getCurrentState());
        consumer.consume();
        System.out.println(consumer.getCurrentState());
        consumer.consume();
        System.out.println(consumer.getCurrentState());
        System.out.println(consumer.beginShutDown());
        System.out.println(consumer.getCurrentState());
        System.out.println(consumer.consume());
        System.out.println(consumer.getCurrentState());
    }

    private Connection getConnection() throws IOException {
        Connection connection = Mockito.mock(Connection.class);
        Table table = Mockito.mock(Table.class);
        ResultScanner scanner = Mockito.mock(ResultScanner.class);
        when(scanner.iterator()).thenReturn(new Iterator<Result>() {
            @Override
            public boolean hasNext() {
                return false;
            }
            @Override
            public Result next() {
                return null;
            }

            @Override
            public void remove() {
            }
        });
        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(connection.getTable(any(TableName.class))).thenReturn(table);
        when(connection.getTable(any(TableName.class), any(ExecutorService.class))).thenReturn(table);
        return connection;
    }
}
