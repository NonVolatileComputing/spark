package org.apache.spark.rdd
import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{SerializableConfiguration, Utils}
import com.intel.bigdatamem._
import com.intel.bigdatamem.collections._
import scala.collection.JavaConversions._


/**
 * An RDD that reads from checkpoint files previously written to reliable storage.
 */
private[spark] class PMemLinkedDSRDD[T: ClassTag](
    @transient sc: SparkContext, 
    val dsPathStr: String, 
    val gfTypes: Array[GenericField.GType], 
    val efProxies: Array[EntityFactoryProxy], 
    val slotKeyId: Long)
  extends PMemRDDLike[T](sc, dsPathStr) {
  
  @transient protected val dsPath = new Path(dsPathStr)

  def genDS(newParts: Int, partSize: Long, genDSPart: (Int, Path, Long, Long)=>Unit, overwrite:Boolean):Boolean = {
    if (getPartitions.length > 0 && overwrite) {
      partitions_ = null
      fs.delete(dsPath, true)
    } 
    fs.mkdirs(dsPath)
    fs.create(new Path(dsPath, "_FAILURE"), true)
    fs.delete(new Path(dsPath, "_SUCCESS"), false)
    if (0 == getPartitions.length) {
      (0 until newParts).foreach {i=>
        genDSPart(i, new Path(dsPath, PMemRDDLike.PMemPartFileName(i)), partSize, slotKeyId)
      }
    }
    fs.delete(new Path(dsPath, "_FAILURE"), false)
    fs.create(new Path(dsPath, "_SUCCESS"), true)
    getPartitions.length == newParts
  }
  
  /**
   * Return the locations of the checkpoint file associated with the given partition.
   */
  protected override def getPreferredLocations(split: Partition): Seq[String] = {
    val status = fs.getFileStatus(
      new Path(dsPathStr, PMemRDDLike.PMemPartFileName(split.index)))
    val locations = fs.getFileBlockLocations(status, 0, status.getLen)
    locations.headOption.toList.flatMap(_.getHosts).filter(_ != "localhost")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    PMemLinkedDSRDD.loadDSPart(new Path(dsPathStr, PMemRDDLike.PMemPartFileName(split.index)), gfTypes, efProxies, slotKeyId, context)
  }

}

private[spark] object PMemLinkedDSRDD extends Logging {

    def loadDSPart[T](
      fullPartName: Path,
      gfTypes: Array[GenericField.GType], 
      efProxies: Array[EntityFactoryProxy],
      slotKeyId: Long,
      context: TaskContext): Iterator[T] = {
        //System.err.println("Start to new allocator for %s".format(fullPartName.toString()))
        val act = new BigDataPMemAllocator(0, fullPartName.toString(), true)
        //System.err.println("Finished new allocator for %s".format(fullPartName.toString()))
        context.addTaskCompletionListener(context => act.close())
        val handler = act.getPersistKey(slotKeyId)
        val linkedvals:PersistentNodeValue[T] = 
          PersistentNodeValueFactory.restore(act, efProxies, gfTypes, handler, false)
        new InterruptibleIterator[T](context, linkedvals.iterator())
    }
}
