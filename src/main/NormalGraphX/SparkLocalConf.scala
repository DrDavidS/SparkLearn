package NormalGraphX

import org.apache.spark.{SparkConf, SparkContext}

case class SparkLocalConf(
                           sc: SparkContext = new SparkContext(
                             new SparkConf().setMaster("local").setAppName("GraphX Ops")
                           )
                         )
