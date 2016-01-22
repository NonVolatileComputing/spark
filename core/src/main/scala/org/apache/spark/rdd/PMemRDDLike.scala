package org.apache.spark.rdd

/**
 * @author wg
 */

import java.io.IOException

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * An RDD partition used to recover checkpointed data.
 */
private[spark] class PMemRDDLikePartition(val index: Int) extends Partition

/**
 * An RDD that recovers checkpointed data from storage.
 */
private[spark] abstract class PMemRDDLike[T: ClassTag](
    @transient sc: SparkContext, dslikePathStr: String)
  extends RDD[T](sc, Nil) {

  @transient protected var partitions_ : Array[Partition] = null
  @transient protected val hadoopConf = sc.hadoopConfiguration
  @transient protected val dslikePath = new Path(dslikePathStr)
  @transient protected val fs = dslikePath.getFileSystem(hadoopConf)
  protected val broadcastedConf = sc.broadcast(new SerializableConfiguration(hadoopConf))

//  require(fs.exists(cpath), s"Checkpoint directory does not exist: $vecDSPath")
  /**
   * Return the path of the checkpoint directory this RDD reads data from.
   */
  def getDSFile: Option[String] = Some(dslikePathStr)
  
  /**
   * Return partitions described by the files in the checkpoint directory.
   *
   * Since the original RDD may belong to a prior application, there is no way to know a
   * priori the number of partitions to expect. This method assumes that the original set of
   * checkpoint files are fully preserved in a reliable storage across application lifespans.
   */
  protected override def getPartitions: Array[Partition] = {
    if (null == partitions_) {
      if (fs.exists(dslikePath)) {
        val inputFiles = fs.listStatus(dslikePath)
          .map(_.getPath)
          .filter(_.getName.startsWith("PMem-part-"))
          .sortBy(_.toString)
        // Fail fast if input files are invalid
        inputFiles.zipWithIndex.foreach { case (path, i) =>
          if (!path.toString.endsWith(PMemRDDLike.PMemPartFileName(i))) {
            throw new SparkException(s"Invalid checkpoint file: $path")
          }
        }
        partitions_ = Array.tabulate(inputFiles.length)(i => new PMemRDDLikePartition(i))
      } else {
        partitions_ = Array()
      }
    }
    return partitions_;
}  
  
  override def compute(p: Partition, tc: TaskContext): Iterator[T] = ???
  // scalastyle:on

}

private[spark] object PMemRDDLike extends Logging {

  /**
   * Return the checkpoint file name for the given partition.
   */
  def PMemPartFileName(partitionIndex: Int): String = {
    "PMem-part-%05d".format(partitionIndex)
  }

}

