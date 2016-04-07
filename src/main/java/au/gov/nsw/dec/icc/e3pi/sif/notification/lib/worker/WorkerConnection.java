package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

class WorkerConnection implements Connection {
    private static final Log LOG = LogFactory.getLog(WorkerConnection.class);

    private final Connection connection;

    WorkerConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Configuration getConfiguration() {
        return connection.getConfiguration();
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
        return connection.getTable(tableName);
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
        return connection.getTable(tableName, pool);
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return connection.getBufferedMutator(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
        return connection.getBufferedMutator(params);
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        return connection.getRegionLocator(tableName);
    }

    @Override
    public Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

    @Override
    public void close() throws IOException {
        LOG.info("Do not close connection.");
    }

    @Override
    public boolean isClosed() {
        return connection.isClosed();
    }

    @Override
    public void abort(String why, Throwable e) {
        connection.abort(why, e);
    }

    @Override
    public boolean isAborted() {
        return connection.isAborted();
    }
}
