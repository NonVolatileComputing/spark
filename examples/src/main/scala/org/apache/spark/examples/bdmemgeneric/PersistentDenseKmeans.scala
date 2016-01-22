package org.apache.spark.examples.bdmemgeneric
import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import com.intel.bigdatamem._
import com.intel.bigdatamem.collections._
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.util.Arrays
import org.apache.spark.examples.mllib._

/**
 * An example k-means app. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DenseKMeans [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object PersistentDenseKmeans {

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import InitializationMode._

  case class Params(
      input: String = null,
      k: Int = -1,
      numIterations: Int = 10,
      initializationMode: InitializationMode = Parallel) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DenseKMeans") {
      head("Persistent DenseKMeans: an example k-means app for dense data.")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[String]("initMode")
        .text(s"initialization mode (${InitializationMode.values.mkString(",")}), " +
        s"default: ${defaultParams.initializationMode}")
        .action((x, c) => c.copy(initializationMode = InitializationMode.withName(x)))
      arg[String]("<input>")
        .text("input paths to examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"Persistent DenseKMeans with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)
    
    val slotid = 10
    
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
    var pmds = sc.pmemLinkedDS[PersistentNodeValue[Double]](params.input, gftypes, efproxies, slotid)
    val examples = pmds.map{x=>Vectors.dense(x.to[Array])}.cache()

    val numExamples = examples.count()

    println(s"numExamples = $numExamples.")

    val initMode = params.initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }

    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(params.k)
      .setMaxIterations(params.numIterations)
      .run(examples)

    val cost = model.computeCost(examples)

    println(s"Total cost = $cost.")

    sc.stop()
  }
}
// scalastyle:on println