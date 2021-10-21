package ReadDataOnHive

import org.apache.spark.sql.{SaveMode, SparkSession}

object RunData {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()

    val options = Utils.bizDate(args, Map("degree" -> CommandItem(true, "度")))
    val data = new ReadData(sparkSession, op = options)
    import sparkSession.implicits._
    val df = data.readHive.vertices.map(row => {
      (row._1, "")
    }).toDF("id", "values")
    df.write.mode(SaveMode.Overwrite).saveAsTable(s"temp_xxxx_${options.dt}")
  }
}
