package dc.streaming.sink

import java.sql._

import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Created by Dingzhenying on 2019/1/22
  */
class JdbcForeachWriter(url:String, user:String, pwd:String, driver:String, tableName:String) extends ForeachWriter[Row]{
  var statement:Statement = _
  var connection:Connection  = _
  //创建连接
  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url,user,pwd)
    this.statement = connection.createStatement()
    true
  }
  //执行sql
  override def process(value: Row): Unit = {
   val sql = "insert into "+tableName+" values('"+value.getAs("t")+"','"+value.getAs("namespace")+"','"+value.getAs("pointId")+"','"+value.getAs("v")+"','0','"+value.getAs("upper_bound")+"','"+value.getAs("lower_bound")+"','0',"+value.getAs("state")+")"
   println(sql)
    statement.executeUpdate(sql)
  }
  //关闭资源
  override def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }
}