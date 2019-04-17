package dc.streaming.sink

/**
  * Created by jiangbaining
  * Date:2019/1/31
  */
object Test {
  def main(args: Array[String]): Unit = {
    def catalog: String =
      s"""{
         |"table":{"namespace":"default", "name":"tableTest"},
         |"rowkey":"key",
         |"columns":{
         |"rowKey":{"cf":"rowkey", "col":"key", "type":"string"},
         |"v":{"cf":"info", "col":"v", "type":"double"},
         |"t":{"cf":"info", "col":"t", "type":"long"}
         |}
         |}""".stripMargin
    println(Option[String](catalog).map(_.toString).getOrElse(""))
    val tableName = "default:tableTest"
    val rowKey="key"
    val coumns="""{"cf":"rowkey", "col":"key", "type":"string"},"v":{"cf":"info", "col":"v", "type":"double"},"t":{"cf":"info", "col":"t", "type":"long"}"""
    //        val hBaseCatalog = options.get("hbasecat").map(_.toString).getOrElse("")
  }
}
