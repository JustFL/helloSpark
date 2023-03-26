package exercise

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.net.URI
import org.apache.hadoop.io.IOUtils

object HDFSUtil {
  def writeToHdfs(content: String): Unit = {

    val conf: Configuration = new Configuration()
    // 追加内容必须配置
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
    val fs: FileSystem = FileSystem.get(new URI("hdfs://bigdata02:8020"), conf, "root")

    // 读取数据使用输入流
    val in: InputStream = new ByteArrayInputStream(content.getBytes)

    // 写入Hdfs使用输出流
    val path: Path = new Path("/HdfsStream.java")
    var out: FSDataOutputStream = null
    if (!fs.exists(path)) {
      out = fs.create(path)
    } else {
      // 如果文件存在 则追加内容
      out = fs.append(path)
    }

    //上传文件
    IOUtils.copyBytes(in, out, 4096)

    in.close()
    out.close()
    fs.close()
  }

  def main(args: Array[String]): Unit = {
    writeToHdfs("hello world")
  }
}
