import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object CodeReview {
  def main(args:Array[String]) ={
    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._

    val df = spark.read.json("C/rnu_4_5_json") //очень большая таблица > 10 ТБ

    val dt_accountCount = df.groupBy("id_pk").agg(count("dt_account").as("dtCount")).filter(col("id_pk") =!= 117)

    val kt_accountCount = df.groupBy("id_pk").agg(count("kt_account").as("ktCount")).filter(col("id_pk") =!= 117)

    val dt_max = df.groupBy("id_pk").agg(max("dt_account").as("dtCountMax"))

    val kt_max = df.groupBy("id_pk").agg(max("kt_account").as("ktCountMax"))

    val resultDf = dt_accountCount
      .join(kt_accountCount, Seq("id_pk"))
      .join(dt_max, Seq("id_pk"))
      .join(kt_max, Seq("id_pk"))
      .filter(col("id_pk") =!= 7747)

    val df2Dict = spark.read.json("C/dictionary") // маленькая таблица, меньше 10 мб

    val ListFromDF2 = df2Dict.select(col("id_pk")).map(x => x.getLong(0).toString).collect().toList

    val veryFinalResult = resultDf.filter(!col("id_pk").isin(ListFromDF2:_*))

    veryFinalResult.show(10, false)

    val dict2 = spark.read.json("C/dict2") // маленькая таблица, меньше 10 мб

    veryFinalResult.join(dict2, Seq("id_pk"), "left").withColumn("izz_number", when(col("izz_number").isNull, "dssf111").otherwise(col("izz_number")))

    veryFinalResult.write.csv("folder")
  }
}
