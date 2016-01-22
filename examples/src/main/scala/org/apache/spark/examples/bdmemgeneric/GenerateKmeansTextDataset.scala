package org.apache.spark.examples.bdmemgeneric

import com.intel.bigdatamem._
import com.intel.bigdatamem.collections._
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.util.Arrays


/**
 * @author wg
 */
object GenerateKmeansTextDataset {
  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: GenerateKmeansTextDataset <slot_id> <par_infilepath> <par_outfilepath>")
      System.exit(1)
    }
    val slotid = args(0).toLong
    val parinfp = args(1)
    val paroutfp = args(2)

    val sparkConf = new SparkConf().setAppName("GenerateKmeansDataset")
    val sc = new SparkContext(sparkConf)

    val gftypes = Array(GenericField.GType.DURABLE, GenericField.GType.DOUBLE)
    val  proxy_elem = new EntityFactoryProxy() with Serializable {
      override def restore[A <: CommonPersistAllocator[A]](allocator:A,
        factoryproxys:Array[EntityFactoryProxy], gfields:Array[GenericField.GType], phandler:Long, autoreclaim:Boolean):Durable = {
        var val_efproxies:Array[EntityFactoryProxy] = null
        var val_gftypes:Array[GenericField.GType] = null
        if ( null != factoryproxys && factoryproxys.length >= 2 ) {
          val_efproxies = Arrays.copyOfRange(factoryproxys, 1, factoryproxys.length);
        }
        if ( null != gfields && gfields.length >= 2 ) {
          val_gftypes = Arrays.copyOfRange(gfields, 1, gfields.length);
        }
        PersistentNodeValueFactory.restore(allocator, val_efproxies, val_gftypes, phandler, autoreclaim)
      }
    }
    val efproxies:Array[EntityFactoryProxy] = Array(proxy_elem)
    
    var pmds = sc.pmemLinkedDS[PersistentNodeValue[Double]](parinfp, gftypes, efproxies, slotid)
    
    pmds.map{x=> x.iterator().mkString(" ")}.saveAsTextFile(paroutfp)
  
  }
}