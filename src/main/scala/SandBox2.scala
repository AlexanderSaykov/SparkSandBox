
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession


import org.apache.log4j.Logger
import org.apache.log4j.Level



import scala.collection.mutable.ListBuffer

object SandBox2 {
  def main(args: Array[String]): Unit = {
    1
    2
    4
    val spark = SparkSession.builder.config("spark.driver.memory", "10G").appName("SandBox2").master("local").getOrCreate()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    def getPaymentPurposeDf: DataFrame ={
      spark
        .read
        .option("multiline", value = true)
        .json("payment_purpose_filter.json")
    }


   println(
     getPaymentPurposeDf
      .filter(col("asnu_code_fk") === 9000)
      .filter(col("tax_doc_type_code") === "sheet_302")
      .filter(col("use_type") === "INCLUDE")
      .select("payment_purpose_pattern")
      .distinct()
      .collect()
      .map(_.getString(0))
      .foldRight(lit(1))(col("payment_purpose").startsWith(_) || _)

     //.foldRight(lit(false))(col("payment_purpose").startsWith(_) || _)
      )

    /*val donuts: Seq[String] = Seq("Plain", "Strawberry", "Glazed")
    def concatDonuts (a:String, b:String): String = {a + " Donut " + b}

    print(donuts.foldRight("1")(concatDonuts))*/
  }

}
