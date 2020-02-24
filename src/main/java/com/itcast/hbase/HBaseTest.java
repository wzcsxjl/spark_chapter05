package com.itcast.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * todo: HBase API操作
 */
public class HBaseTest {

    // 初始化Configuration配置对象
    private Configuration conf = null;
    // 初始化连接对象
    private Connection conn = null;

    /**
     * 连接集群
     *
     * @throws IOException
     */
    @Before // 用于Junit单元测试中控制程序最先执行的注解，在这里保证初始化init()方法在程序中是最先执行的
    public void init() throws IOException {
        // 获取Configuration配置对象
        conf = HBaseConfiguration.create();
        // 对HBase客户端来说，只需知道HBase所经过的Zookeeper集群地址即可（因为HBase的客户端找HBase读写数据完全不用经过HMaster）
        conf.set("hbase.zookeeper.quorum", "node-1:2181,node-2:2181,node-3:2181");
        // 获取连接
        conn = ConnectionFactory.createConnection(conf);
    }

    /**
     * 创建数据表
     *
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        // 获取表管理器对象
        Admin admin = conn.getAdmin();
        // 创建表的描述对象并指定表名
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("t_user_info".getBytes()));
        // 构造第一个列族描述对象并指定列族名
        HColumnDescriptor hcd1 = new HColumnDescriptor("base_info");
        // 构造第二个列族描述对象并指定列族名
        HColumnDescriptor hcd2 = new HColumnDescriptor("extra_info");
        // 为该列族设定一个版本数量，最小为1，最大为3
        hcd2.setVersions(1, 3);
        // 将列族描述对象添加到表描述对象中
        tableDescriptor.addFamily(hcd1).addFamily(hcd2);
        // 利用表管理器来创建表
        admin.createTable(tableDescriptor);
        // 关闭表管理器和连接对象，避免资源浪费
        admin.close();
        conn.close();
    }

    /**
     * 插入数据
     *
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        // 创建Table对象，通过Table对象来添加数据
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        // 创建一个集合，用于存放Put对象
        ArrayList<Put> puts = new ArrayList<>();
        // 构建Put对象（KV形式）并指定其行键，用于构建表中的行和列
        Put put01 = new Put(Bytes.toBytes("user001"));
        put01.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));
        put01.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("password"), Bytes.toBytes("123456"));
        Put put02 = new Put("user002".getBytes());
        put02.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("lisi"));
        put02.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));
        // 把所有的put对象添加到一个集合中
        puts.add(put01);
        puts.add(put02);
        // 通过表对象table提交所有插入数据的记录
        table.put(puts);
        // 关闭表对象
        table.close();
        // 关闭连接对象
        conn.close();
    }

    /**
     * 查看指定字段的数据（演示查看行键为user001的数据）
     *
     * @throws IOException
     */
    @Test
    public void testGet() throws IOException {
        // 获取一个Table对象，指定数据表为“t_user_info”
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        // 创建get查询参数对象，指定要获取的是哪一行
        Get get = new Get("user001".getBytes());
        // 返回查询结果的数据
        Result result = table.get(get);
        // 获取结果中的所有cell
        List<Cell> cells = result.listCells();
        // 遍历所有的cell
        for (Cell cell : cells) {
            // 获取行键
            System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
            // 得到列族
            System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列名：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        // 关闭表对象
        table.close();
        // 关闭连接对象
        conn.close();
    }

    /**
     * 扫描数据
     * @throws IOException
     */
    @Test
    public void testScan() throws IOException {
        // 获取Table对象，指定要查看的数据表为“t_user_info”
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        // 创建全表扫描对象scan
        Scan scan = new Scan();
        // 获取查询的数据，通过表对象table调用getScanner方法扫描表中的所有数据
        ResultScanner scanner = table.getScanner(scan);
        // 获取ResultScanner所有数据，返回迭代器（将扫描到的所有数据存放到迭代器中）
        Iterator<Result> iter = scanner.iterator();
        // 遍历迭代器
        while (iter.hasNext()) {
            // 获取当前每一行结果数据
            Result result = iter.next();
            // 获取当前每一行中所有的cell对象
            List<Cell> cells = result.listCells();
            // 迭代所有的cell
            for (Cell cell : cells) {
                // 获取行键
                byte[] rowArray = cell.getRowArray();
                // 获取列族
                byte[] familyArray = cell.getFamilyArray();
                // 获取列族下的列名称
                byte[] qualifierArray = cell.getQualifierArray();
                // 获取列字段的值
                byte[] valueArray = cell.getValueArray();
                // 打印rowArray、familyArray、qualifierArray、valueArray
                System.out.println("行键：" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
                System.out.print("列族：" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
                System.out.print(" " + "列名：" + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.println(" " + "值：" + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("------------------------------");
        }
        // 关闭
        table.close();
        conn.close();
    }

    /**
     * 删除指定列的数据
     * @throws IOException
     */
    @Test
    public void testDel() throws IOException {
        // 获取table对象
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        // 获取删除对象delete，需要一个rowkey，指定要删除的行键为user001
        Delete delete = new Delete("user001".getBytes());
        // 在delete对象中指定要删除的列族-列名称
        delete.addColumn("base_info".getBytes(), "password".getBytes());
        // 通过表对象table调用delete()方法执行删除操作
        table.delete(delete);
        // 关闭表对象和连接对象释放资源
        table.close();
        conn.close();
    }

    /**
     * 删除表
     * @throws IOException
     */
    @Test
    public void testDrop() throws IOException {
        // 获取一个表的管理器对象admin
        Admin admin = conn.getAdmin();
        // 删除表时先需要disable，将表置为不可用，然后再delete
        // 通过表管理器对象admin调用disableTable()方法将表t_user_info设置为不可用状态
        admin.disableTable(TableName.valueOf("t_user_info"));
        // 通过表管理器对象admin调用deleteTable()方法执行删除表操作
        admin.deleteTable(TableName.valueOf("t_user_info"));
        // 关闭表管理器对象
        admin.close();
        // 关闭连接对象
        conn.close();
    }

}
