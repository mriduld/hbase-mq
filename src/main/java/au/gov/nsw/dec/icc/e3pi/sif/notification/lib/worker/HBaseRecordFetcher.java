package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;
import au.gov.nsw.dec.icc.e3pi.sif.notification.model.Record;
import au.gov.nsw.dec.icc.e3pi.sif.notification.model.RecordsResult;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

class HBaseRecordFetcher {
    private static final Log LOG = LogFactory.getLog(HBaseRecordFetcher.class);

    private static final TableName TABLE_NAME = TableName.valueOf("sif:util.notification");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("a");
    private static final byte[] POSTFIX = new byte[]{0x00};

    private final Connection connection;
    private final IPartition partition;
    private final ExecutorService executorService;
    private final byte[] prefix;


    private byte[] nextIterator;
    private boolean isInitialized;

    HBaseRecordFetcher(IPartition partition,
                       Connection connection,
                       ExecutorService executorService) {
        this.partition = partition;
        this.connection = connection;
        this.executorService = executorService;
        this.prefix = DigestUtils.md5(Bytes.toBytes(partition.getId()));
    }

    RecordsResult getRecords(int maxRecords) throws IOException {
        if (!isInitialized) {
            throw new IllegalArgumentException("HBaseRecordFetcher.getRecords called before initialization.");
        }
        if(LOG.isDebugEnabled()){
            LOG.info("Scanning "+ partition + " for next set of records");
        }
        RecordsResult recordsResult = get(maxRecords);
        nextIterator = recordsResult.getLastIterator();
        return recordsResult;
    }

    void initialize(byte[] nextIterator) {
        this.nextIterator = nextIterator;
        this.isInitialized = true;
    }

    private RecordsResult get(int maxRecords) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(COLUMN_FAMILY);
        scan.setRowPrefixFilter(prefix);
        byte[] startRow = nextIterator.length == 0 ? prefix : Bytes.add(nextIterator, POSTFIX);
        scan.setStartRow(startRow);
        scan.setCaching(maxRecords);
        scan.setCacheBlocks(false);
        scan.setFilter(new PageFilter(maxRecords));

        List<Record> records = new ArrayList<>();
        byte[] lastScanned = nextIterator;

        Table table = null;
        ResultScanner scanner = null;
        try{
            table = connection.getTable(TABLE_NAME, executorService);
            scanner = table.getScanner(scan);
            for(Result r: scanner){
                byte[] row = r.getRow();
                Map<byte[], byte[]> data = r.isEmpty() ?  Collections.<byte[], byte[]>emptyMap() : r.getNoVersionMap().firstEntry().getValue();
                records.add(new Record(row, data));
                lastScanned = row;
            }
        } finally {
            CloseableUtils.closeQuietly(table);
            CloseableUtils.closeQuietly(scanner);
        }
        if(LOG.isDebugEnabled()) {
            LOG.info("Found " + records.size() + " records for " + partition);
        }
        return new RecordsResult(records, lastScanned);
    }
}
