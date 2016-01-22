package org.apache.spark.examples.bdmemgeneric

import com.intel.bigdatamem._
import com.intel.bigdatamem.collections._
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.util.Arrays
import breeze.linalg.{Vector, DenseVector, squaredDistance}
import org.apache.spark.mllib.linalg.Vectors

/**
 * @author wg
 */
object LoadComparison {
  
  def time[R](hint:String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("%s Elapsed time: %d ns, %d ms, %d secs".format(hint, (t1 - t0), (t1 - t0)/1000/1000, (t1 - t0)/1000/1000/1000))
    result
  }
  
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }

  
  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: GenerateKmeansTextDataset <slot_id> <par_persistmem_filepath> <par_text_filepath>")
      System.exit(1)
    }
    val slotid = args(0).toLong
    val parpmfp = args(1)
    val partextfp = args(2)
    
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
    println("Starting the Persistent Memory Loading")
    Thread.sleep(3000);
    println("Persistent loaded %d items".format(time("Persistent Load Time: ", 
        { var pmds = sc.pmemLinkedDS[PersistentNodeValue[Double]](parpmfp, gftypes, efproxies, slotid)
          pmds.map{x=>DenseVector(x.to[Array])}.count() 
        })))
    
    println("Starting the Text Loading")
    Thread.sleep(3000);
    println("Text loaded %d items".format(time("Text Load Time: ", {
      val lines = sc.textFile(partextfp, 20)
      lines.map(parseVector _).count()})))
    
  }
  
  
}