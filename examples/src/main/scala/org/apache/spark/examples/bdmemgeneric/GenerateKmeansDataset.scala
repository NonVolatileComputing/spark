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
object GenerateKmeansDataset {
  
  def genDSPartWithLinkedNodeValueDouble
    (paritemcnt: Int, vecdim: Int)
    (index: Int, fullPartName:Path, partSize: Long, slotKeyId: Long):Unit = {
  
    val rand = com.intel.bigdatamem.Utils.createRandom()
    val m_act = new BigDataPMemAllocator(partSize, fullPartName.toString(), true)

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
    
    val elem_gftypes = Array(GenericField.GType.DOUBLE)
    val elem_efproxies:Array[EntityFactoryProxy] = null

    var next, pre_next:PersistentNodeValue[PersistentNodeValue[Double]] = null

    var elem, pre_elem, first_elem:PersistentNodeValue[Double] = null
    
    var linkhandler: Long = 0L
    
    pre_next = null
    (0 until paritemcnt).foreach { _=>
      first_elem = null
      pre_elem = null
      (0 until vecdim).foreach { _ =>
        elem = PersistentNodeValueFactory.create(m_act, elem_efproxies, elem_gftypes, false)
        elem.setItem(rand.nextDouble(), false)
        if (null == pre_elem) {
          first_elem = elem
        } else {
          pre_elem.setNext(elem, false)
        }
        pre_elem = elem;
      }
      
      next =  PersistentNodeValueFactory.create(m_act, efproxies, gftypes, false);
      next.setItem(first_elem, false)
      if (null == pre_next) {
        linkhandler = next.getPersistentHandler
      } else {
        pre_next.setNext(next, false);
      }
      pre_next = next;
    }
  
    m_act.setPersistKey(slotKeyId, linkhandler)
    m_act.close
  }
  
  def main(args: Array[String]) {

    if (args.length < 6) {
      System.err.println("Usage: GenerateKmeansDataset <slot_id> <par_filepath> <par_count> <par_size> <paritem_count> <vector_dimension>")
      System.exit(1)
    }
    val slotid = args(0).toLong
    val parfp = args(1)
    val parcnt = args(2).toInt
    val parsz = args(3).toLong
    val paritemcnt = args(4).toInt
    val vecdim = args(5).toInt

    val sparkConf = new SparkConf().setAppName("GenerateKmeansDataset")
    val sc = new SparkContext(sparkConf)

    val gftypes = Array(null, GenericField.GType.DURABLE, GenericField.GType.DOUBLE)
    val  proxy_elem = new EntityFactoryProxy() with Serializable {
      override def restore[A <: CommonPersistAllocator[A]](allocator:A,
        factoryproxys:Array[EntityFactoryProxy], gfields:Array[GenericField.GType], phandler:Long, autoreclaim:Boolean):Durable = {
        PersistentNodeValueFactory.restore(allocator, factoryproxys, gfields, phandler, autoreclaim)
      }
    }
    val efproxies:Array[EntityFactoryProxy] = Array(null, proxy_elem)

    val pmds = sc.pmemLinkedDS[PersistentNodeValue[Double]](parfp, gftypes, efproxies, slotid) 
  
    pmds.genDS(parcnt, parsz, genDSPartWithLinkedNodeValueDouble(paritemcnt, vecdim) _, true)
  }
}