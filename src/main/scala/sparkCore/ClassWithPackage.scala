package sparkCore

import java.net.URL

object ClassWithPackage {
  def main(args: Array[String]): Unit = {

    val url: URL = testGetResource("")
    println(url)
    val url1: URL = testGetResource("ClassWithPackage.class")
    println(url1)
    val url2: URL = testGetResource("wc.txt")
    println(url2)
    val url3: URL = testGetResource("/")
    println(url3)
    val url4: URL = testGetResource("/ClassWithPackage.class")
    println(url4)
    val url5: URL = testGetResource("/wc.txt")
    println(url5)
  }

  def testGetResource(p: String): URL = ClassWithPackage.getClass.getResource(p)
}
