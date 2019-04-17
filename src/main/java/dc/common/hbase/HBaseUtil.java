package dc.common.hbase;


import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.util.*;

public class HBaseUtil {

    public static HBaseAdmin getHBaseAdmin(String hbase_zookeeper_quorum) throws IOException{
        HBaseAdmin hbaseAdmin = null;
        try {
            hbaseAdmin = (HBaseAdmin) HBaseConn.getInstance().getConnection(hbase_zookeeper_quorum).getAdmin();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
        return hbaseAdmin;
    }


    public static NamespaceDescriptor[] listNamespaces(String hbase_zookeeper_quorum){
        NamespaceDescriptor[] array = null;
        try {
            HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
            array = admin.listNamespaceDescriptors();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return array;
    }

    public static NamespaceDescriptor getNamespace(String hbase_zookeeper_quorum,String name){
        NamespaceDescriptor namespaceDescriptor= null;
        try {
            HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
            namespaceDescriptor = admin.getNamespaceDescriptor(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return namespaceDescriptor;
    }

    public static void createNamespace(String hbase_zookeeper_quorum,String name){
        boolean isExist=false;
        NamespaceDescriptor[] array =listNamespaces(hbase_zookeeper_quorum);
        for(int i=0;i<array.length;i++){
            NamespaceDescriptor ns=array[i];
            if(ns.getName().equals(name)){
                isExist=true;
                break;
            }
        }
        if(!isExist) {
            NamespaceDescriptor ns = NamespaceDescriptor.create(name).build();
            try {
                HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
                admin.createNamespace(ns);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 获取表实例
     *
     * @param tableName 表名 namespaceName:tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String hbase_zookeeper_quorum,String tableName) throws IOException {
        return HBaseConn.getInstance().getConnection(hbase_zookeeper_quorum).getTable(TableName.valueOf(tableName));
    }

    /**
     * 创建HBase表
     *
     * @param tableName 表名  namespaceName:tableName
     * @param cfs       列族的数组
     * @return 是否创建成功
     */
    public static boolean createTable(String hbase_zookeeper_quorum,String tableName, String[] cfs) {
        try {
            HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
            if (admin.tableExists(tableName)) {
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                //columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    public static boolean createTable(String hbase_zookeeper_quorum,String tableName, String[] cfs,  byte [][] splitKeys) {
        try {
            HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
            if (admin.tableExists(tableName)) {
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                //columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor,splitKeys);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    public static void createTableWithRegions(String hbase_zookeeper_quorum,String tableName, String[] cfs,int regionNum) {

        byte[][] keys=new byte[regionNum][];
        for(int i=0;i<regionNum;i++){
            String key;
            if(i<10){
                key="0"+i;
            }else key=String.valueOf(i);
            keys[i]= Bytes.toBytes(key);
        }
        createTable(hbase_zookeeper_quorum,tableName,cfs,keys);
    }

    public static void createTableWith100Regions(String hbase_zookeeper_quorum,String tableName, String[] cfs) {
        createTableWithRegions(hbase_zookeeper_quorum,tableName,cfs,100);
    }


    /**
     * 删除表
     *
     * @param tableName
     * @return
     */
    public static boolean deleteTable(String hbase_zookeeper_quorum,String tableName) {
        try {
            HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static boolean truncateTable(String hbase_zookeeper_quorum,String tableName) {
        try {
            HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
            admin.disableTable(tableName);
            admin.truncateTable(TableName.valueOf(tableName),true);
            //删完数据自动enable
           // admin.enableTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }



    public static TableName[] listTableNames(String hbase_zookeeper_quorum){
        try {
            HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
            return admin.listTableNames();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static TableName[] listTableNamesByNamespace(String hbase_zookeeper_quorum,String namespace){
        try {
            HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
            return admin.listTableNamesByNamespace(namespace);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 插入一条数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     * @param family    列族名
     * @param qualifier 列标识
     * @param value      数据
     * @return 是否插入成功
     */
    public static boolean putRow(String hbase_zookeeper_quorum,String tableName, String rowKey, String family, String qualifier, double value) {

        try(Table table = getTable(hbase_zookeeper_quorum,tableName)) {
            //Table table = getTable(hbase_zookeeper_quorum,tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 批量插入数据
     *
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putRows(String hbase_zookeeper_quorum,String tableName, List<Put> puts) {
        try(Table table = getTable(hbase_zookeeper_quorum,tableName)) {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 获取单条数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     * @return 查询结果
     */
    public static Result getRow(String hbase_zookeeper_quorum,String tableName, String rowKey) {
        try(Table table = getTable(hbase_zookeeper_quorum,tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 利用过滤器获取单条数据
     *
     * @param tableName
     * @param rowKey
     * @param filterList
     * @return
     */
    public static Result getRow(String hbase_zookeeper_quorum,String tableName, String rowKey, FilterList filterList) {
        try(Table table = getTable(hbase_zookeeper_quorum,tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filterList);
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * scan扫描表操作
     *
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String hbase_zookeeper_quorum,String tableName) {
        try( Table table = getTable(hbase_zookeeper_quorum,tableName)) {
            Scan scan = new Scan();
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 批量检索数据
     *
     * @param tableName   表名
     * @param startRowKey 起始RowKey
     * @param endRowKey   终止RowKey
     * @return resultScanner
     */
    public static ResultScanner getScanner(String hbase_zookeeper_quorum,String tableName, String startRowKey, String endRowKey) {
        try(Table table = getTable(hbase_zookeeper_quorum,tableName)) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 过滤扫描
     *
     * @param tableName
     * @param startRowKey
     * @param endRowKey
     * @param filterList
     * @return
     */
    public static ResultScanner getScanner(String hbase_zookeeper_quorum,String tableName, String startRowKey, String endRowKey, FilterList filterList) {
        try(Table table = getTable(hbase_zookeeper_quorum,tableName)) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除一条记录
     *
     * @param tableName 表名
     * @param rowKey    唯一标识行
     * @return 是否删除成功
     */
    public static boolean deleteRow(String hbase_zookeeper_quorum,String tableName, String rowKey) {
        try( Table table = getTable(hbase_zookeeper_quorum,tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 删除列族
     *
     * @param tableName
     * @param family
     * @return
     */
    public static boolean deleteColumnFamily(String hbase_zookeeper_quorum,String tableName, String family) {
        try {
            HBaseAdmin admin = getHBaseAdmin(hbase_zookeeper_quorum);
            admin.deleteColumn(tableName, family);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean deleteQualifier(String hbase_zookeeper_quorum,String tableName, String rowKey, String family, String qualifier) {
        try(Table table = getTable(hbase_zookeeper_quorum,tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;

    }

    public static int getRegionByHash(String key, int numRegions){
        return Math.abs(key.hashCode()) % numRegions;
    }

    public static String getMD5HashRowkey(String uri,long timestamp){
        return MD5Hash.getMD5AsHex(uri.getBytes())+timestamp;
    }

    /**
     * String.format("%02d",salt)+ uri + timestamp;//性能低
     * salt取值[0,numRegions-1]，为保证salt是两位数，numRegions最大取值为100
     * 清洗库中的数据都是按分钟取的值，后n位全是0，所以去掉后n位以节省存储空间
     * 读取时需要乘以10^n，见{@link this#getTimeFromRowkey(String)}
     * n=4 {@link this#n}
     * @param uri
     * @param timestamp
     * @param numRegions
     * @return
     */
    private static int n=4;
    public static String getHashRowkeyWithSalt(String uri,long timestamp, int numRegions){
        int salt=Math.abs(uri.hashCode()) % numRegions;
        String row;
        if(salt<10)row = "0"+salt+uri+timestamp;//补0
        else row = salt + uri + timestamp;
        return row.substring(0,row.length()-n);
    }

    public static long getTimeFromRowkey(String rowkey){
        return Long.valueOf(rowkey.substring(rowkey.length()-(13-n)))*(int)Math.pow(10,n);
    }

}
