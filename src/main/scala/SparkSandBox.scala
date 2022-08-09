import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSandBox {

  def main(args:Array[String]) = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._


    /** Фильтрация с передачей словаря */
    val data = Seq(
      ("Michael","Rose","Jones","2010","M",4000),
      ("Robert","K","Williams","2010","M",5000),
      ("Maria","Anne","Jones","2005","F",4000)
    )

    val columns = Seq("fname","mname","lname","job_year","gender","salary")

    val myMap: Map[String, Any] = Map(
      "salary" -> 4000,
      "job_year" -> 2010,
      "fname" -> "Michael"
    )

    //Функциональное решение

    val foldedList = myMap.foldLeft(lit(true)) {(accumulator, values) =>
      accumulator && col(values._1) === values._2
    }

    val df = data.toDF(columns:_*)

    df.filter(foldedList).show()

    //Императивное решение решение

    var df2 = data.toDF(columns:_*)

    myMap.foreach{case(key: String, value: Any) => df2 = df2.filter(col(key) === value)}

    df2.show()

  }}
