package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseDao {
    private Admin admin;
    private Connection connection;

    @Before
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "db1,db2,db3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "db1:16000");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    @After
    public void destroy() throws IOException {
        connection.close();
        admin.close();
    }

    @Test
    public void testCreate() throws Exception {
        TableName name = TableName.valueOf("goddess");
        HTableDescriptor desc = new HTableDescriptor(name);
        HColumnDescriptor base_info = new HColumnDescriptor("base_info");
        HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");
        desc.addFamily(base_info);
        desc.addFamily(extra_info);
        admin.createTable(desc);
    }

    @Test
    public void testGet() throws Exception {
        Table table = connection.getTable(TableName.valueOf("goddess"));
        Get get = new Get(Bytes.toBytes("rk0001"));
        get.setMaxVersions(5);
        Result result = table.get(get);
        byte[] bytes = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
        System.out.println(Bytes.toString(bytes));

        for (Cell cell : result.rawCells()) {
            String family = Bytes.toString(cell.getFamilyArray());
            System.out.println(family);
            String qualifier = Bytes.toString(cell.getQualifierArray());
            System.out.println(qualifier);
            String value = Bytes.toString(cell.getValueArray());
            System.out.println(value);
        }
    }

    @Test
    public void testScan() throws Exception {
        Table table = connection.getTable(TableName.valueOf("goddess"));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }

    }

    @Test
    public void testPut() throws Exception {
        Table table = connection.getTable(TableName.valueOf("goddess"));
        Put put = new Put(Bytes.toBytes("rk0001"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("24"));
        put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("point"), Bytes.toBytes("99"));
        table.put(put);
        table.close();
    }

    @Test
    public void testDrop() throws Exception {
        admin.disableTable(TableName.valueOf("goddess"));
        admin.deleteTable(TableName.valueOf("goddess"));
    }
}
