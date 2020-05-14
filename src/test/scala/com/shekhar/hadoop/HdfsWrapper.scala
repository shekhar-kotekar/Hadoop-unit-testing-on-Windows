package com.shekhar.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.{BeforeAndAfterAll, TestSuite}

/**
 * This trait wraps creation of Hadoop mini cluster
 * Any Test class can extend from this and use mini cluster
 */
trait HdfsWrapper extends TestSuite with BeforeAndAfterAll {
  // dfsCluster object will hold reference to Mini HDFS cluster and
  // it can be used by sub classes
  protected var dfsCluster: MiniDFSCluster = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    this.dfsCluster = getHdfsCluster
  }

  /**
   * Creates HDFS configuration and creates Mini DFS cluster
   * @return Object of type MiniDFSCluster
   */
  private def getHdfsCluster: MiniDFSCluster = {
    val hdfsConf: Configuration = new Configuration()
    val numberOfDataNodes: Int = 1
    val clusterBuilder: MiniDFSCluster.Builder =
      new MiniDFSCluster.Builder(hdfsConf).numDataNodes(numberOfDataNodes)
    clusterBuilder.build()
  }
}
