package exercise

import java.io.InputStream
import java.util.Properties

object PropUtil {

  def getProperty(key: String): Object = {
    val input: InputStream = this.getClass.getResourceAsStream("/exercise.properties")
    val props: Properties = new Properties()
    props.load(input)
    input.close()

    props.getProperty(key)
  }
}
