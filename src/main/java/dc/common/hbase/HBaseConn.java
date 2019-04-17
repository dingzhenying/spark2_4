package dc.common.hbase;

import dc.common.util.PropertyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;


public class HBaseConn {
    private static final Logger logger = LoggerFactory.getLogger(HBaseConn.class);
    //private static Configuration configuration;
    private static Connection connection;

    private static class LazyHolder {
        private static final HBaseConn INSTANCE = new HBaseConn();
    }

    private HBaseConn() {
        /*if (configuration == null) {
            configuration = HBaseConfiguration.create();
            //configuration.set("hbase.zookeeper.quorum",  PropertyUtil.getProperty("hbase.zookeeper.quorum"));
        }*/
    }

    /**
     * 创建连接
     *
     * @return
     */
    public  Connection getConnection(String hbase_zookeeper_quorum) {
        if (connection == null || connection.isClosed()) {
            try {
                Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum",  hbase_zookeeper_quorum);
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
        }
        return connection;
    }

    /*public  Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                //configuration.set("hbase.zookeeper.quorum",  hbase_zookeeper_quorum);
                Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum",  PropertyUtil.getProperty("hbase.zookeeper.quorum"));
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
        }
        return connection;
    }*/

    /**
     * 获取数据库连接
     *
     * @return
     */
    public static HBaseConn getInstance() {

        return LazyHolder.INSTANCE;
    }



    /**
     * 关闭连接
     */
    public  void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
        }
    }
}
