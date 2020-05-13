package com.shekhar.hadoop

import org.apache.hadoop.fs.Path
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
    }
  }

}
