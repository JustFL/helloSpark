package exercise

object ChineseCode {
  def main(args: Array[String]): Unit = {
    val str = "这世界那么多人"
    val bytes: Array[Byte] = str.getBytes

    // 变成乱码
    val str1: String = new String(bytes, "ISO8859-1")
    println(str1)

    // 将乱码按照原先字符集转为字节数组
    val bytes1: Array[Byte] = str1.getBytes("ISO8859-1")
    // 将字符数组按照文本原先的编码方式转为字符串
    val str2: String = new String(bytes1, "UTF-8")
    println(str2)
  }
}
