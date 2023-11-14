package eventordering

import java.io.InputStream
import java.util.Properties

object Config {

  private val properties: Properties = {
    val prop = new Properties()
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("application.properties")
    prop.load(inputStream)
    prop
  }

  def getProperty(key: String): String = properties.getProperty(key)
}