package com.github.relmad.cloudera.cca175

package object utils {
  def deleteDir(dirPath : String) : Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(dirPath), hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(dirPath), true) } catch { case _ : Throwable => { } }
  }
}
