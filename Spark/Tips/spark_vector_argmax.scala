package com.xxx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext


object spark_vector_argmax{
  def main(arg: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // create some sample data:
    import org.apache.spark.mllib.linalg.{Vectors,Vector}
    //case class myrow(topics:Vector)
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TopicExtraction")
      .getOrCreate()
    import spark.implicits._                  //必须有，否则$符号不显示
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Array((Vectors.dense(1,0.2),Vectors.dense(0.6,0.2))))
    val mydf = spark.createDataFrame(rdd).toDF("id","topics")
    mydf.show()

    // build the udf
    import org.apache.spark.sql.functions.udf
    val func = udf( (x:Vector) => x.toDense.values.toSeq.indices.maxBy(x.toDense.values) )  //输入数据的格式需要是Vector类型

    mydf.withColumn("max_idx",func($"topics")).show()
  }
}



