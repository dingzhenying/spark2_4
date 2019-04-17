package dc.streaming.util

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by Dingzhenying on 2019/3/13
  */
object GetPropertieInfoUtil {
  val propertiesFilePath="/application.properties"

  def getParameterValue(parameterName:String ): String ={
    val properties = new Properties()
    val proIn=new FileInputStream(this.getClass.getResource(propertiesFilePath).getFile)
    properties.load(proIn)
    properties.get(parameterName).toString
  }

}
