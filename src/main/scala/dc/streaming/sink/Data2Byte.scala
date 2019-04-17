package dc.streaming.sink

import com.alibaba.druid.sql.ast.SQLStructDataType.Field
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by Dingzhenying on 2019/3/16
  */
class Data2Byte(f:Option[Field] = None) {
    def toBytes(input: Any): Array[Byte] = {
      input match {
        case data: Boolean => Bytes.toBytes(data)
        case data: Byte => Array(data)
        case data: Array[Byte] => data
        case data: Double => Bytes.toBytes(data)
        case data: Float => Bytes.toBytes(data)
        case data: Int => Bytes.toBytes(data)
        case data: Long => Bytes.toBytes(data)
        case data: Short => Bytes.toBytes(data)
        case data: UTF8String => data.getBytes
        case data: String => Bytes.toBytes(data)
        case _ => throw new
            UnsupportedOperationException(s"PrimitiveType coder: unsupported data type $input")
      }
    }
}
