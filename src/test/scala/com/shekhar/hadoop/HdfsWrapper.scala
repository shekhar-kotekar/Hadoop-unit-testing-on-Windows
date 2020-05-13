package com.shekhar.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.{BeforeAndAfterAll, TestSuite}

/**
 * This trait wraps creation of Hadoop mini cluster and makes it available to all of its sub classes
 */
trait HdfsWrapper extends TestSuite with BeforeAndAfterAll {

  protected var dfsCluster: MiniDFSCluster = _
  private val hdfsRootDirPath: String =
    new Path(getClass.getClassLoader.getResource("hdfs-root/test.txt").getPath).getParent.toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    this.dfsCluster = getHdfsCluster
  }

  private def getHdfsCluster: MiniDFSCluster = {
    val hdfsConf: Configuration = new Configuration()
    val clusterBuilder: MiniDFSCluster.Builder = new MiniDFSCluster.Builder(hdfsConf)
    clusterBuilder.build()
  }


}
