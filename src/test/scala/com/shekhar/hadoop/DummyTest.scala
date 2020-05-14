package com.shekhar.hadoop

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

/**
 * Test class extends HDFS wrapper trait and makes some simple HDFS related unit test(s)
 */
class DummyTest extends WordSpec with Matchers with HdfsWrapper {

  "a dummy" should {
    "be able to do hdfs operations" in {
      val fs = dfsCluster.getFileSystem
      val fileToCreate: Path = new Path("parent/directory/dummy.txt")
      fs.create(fileToCreate, true)
      fs.exists(fileToCreate) shouldBe true

      val parentDirPath: String = "parent/directory/sub/directory/"
      val fileName: String = "new-file.txt"
      val fileWithContents: Path = new Path(s"$parentDirPath/$fileName")
      writeDataFileToHdfs(fs, new Path(parentDirPath), fileName, "write it to fil eon hdfs")
      fs.exists(fileWithContents)
    }
  }

  /**
   * Companion method which simply writes data to a file on HDFS
   * @param fs HDFS object. It can be retrieved from mini DFS cluster object
   * @param dir directory on HDFS in which file must be written
   * @param fileName Name of file to be written on HDFS
   * @param text content to be written in file.
   */
  private def writeDataFileToHdfs(fs: FileSystem, dir: Path, fileName: String, text: String): Unit = {
    val path: Path = dir.suffix("/" + fileName)
    fs.mkdirs(path.getParent)
    val out: FSDataOutputStream = fs.create(path, true)
    out.writeBytes(text)
    out.flush()
    out.close()
    // Some times mini DFS cluster takes time to write file and test cases might fail
    // So lets sleep thread for a moment before we continue
    Thread.sleep(1000)
  }
}
