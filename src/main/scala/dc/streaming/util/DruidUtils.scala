package dc.streaming.util

import java.util.Properties

import com.alibaba.druid.pool.DruidDataSource
import javax.sql.DataSource

/**
  * Created by shirukai on 2019-02-27 16:41
  * 基于Druid的数据库连接池
  */
object DruidUtils {
  private var ds: Option[DataSource] = None

  def createDataSource(options: Map[String, String]): Option[DataSource] = {
    if (ds.isEmpty) {
      try {
        val properties = new Properties()
        options.foreach(x => properties.setProperty(x._1, x._2))
        val dataSource = new DruidDataSource()
        dataSource.setConnectProperties(properties)
        dataSource.setUsername(options("username"))
        dataSource.setPassword(options("password"))
        dataSource.setUrl(options("jdbcUrl"))
        dataSource.setDriverClassName(options("driverClassName"))
        ds = Some(dataSource)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    ds
  }
}
