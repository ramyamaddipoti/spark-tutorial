package org.examples.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.hive.HiveContext
import scalax.file.Path

object UIDStats {

  val conf = new SparkConf()
    .setAppName("Aadhar Dataset Analysis using Spark")
    .setMaster("local[2]")
  val sc = new SparkContext(conf)

  val hiveContext = new HiveContext(sc)
  import hiveContext.implicits._

  def main(args: Array[String]) {

    val uidEnrolmentDF = hiveContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))

    uidEnrolmentDF.registerTempTable("uid_enrolments_detail")
    
    val path: Path = Path.fromString("output/uid")
    path.deleteRecursively()
    //FileUtils.deleteQuietly(new File("uid/stateWiseCount.csv"))

    val stateWiseCountDF = hiveContext.sql("""
                                           | select state,
                                           |     sum(`Aadhaar generated`) as count
                                           | from  uid_enrolments_detail
                                           | group by state
                                           | order by count desc""".stripMargin)
    
    //stateWiseCountDF.write.mode("overwrite").format("com.databricks.spark.csv").save("uid/stateWiseCount.csv")
    stateWiseCountDF.coalesce(1).write.
      format("com.databricks.spark.csv").option("header", "true").
      save("output/uid/stateWiseCount.csv")

    val maxEnrolmentAgencyDF = hiveContext.sql("""
                                            | select `Enrolment Agency` as Enrolment_Agency,
                                            |     sum(`Aadhaar generated`) as count
                                            | from uid_enrolments_detail
                                            | group by `Enrolment Agency`
                                            | order by count desc""".stripMargin)
      //.collect().foreach(println)
      maxEnrolmentAgencyDF.write.mode("overwrite").format("com.databricks.spark.csv").save("uid/agencyWiseCount.csv")

      val distictWiseGenderWiseDF = hiveContext.sql("""
                                            | select district,
                                            | (case when male_count is null then 0 else male_count end) as male_count,
                                            | (case when female_count is null then 0 else female_count end) as female_count
                                            | from
                                            | (
                                            | select district,
                                            | sum(CASE WHEN Gender='M' THEN `Aadhaar generated` END) as male_count,
                                            | sum(CASE WHEN Gender='F' THEN `Aadhaar generated` END) as female_count
                                            | from uid_enrolments_detail
                                            | group by district
                                            | ) as tbl1
                                            | order by male_count desc, female_count desc
                                            | """.stripMargin)

    // distictWiseGenderWiseDF.write.mode("overwrite").saveAsTable("uid.district_wise_gender_count")
    distictWiseGenderWiseDF.coalesce(1).write.
      format("com.databricks.spark.csv").option("header", "true").
      save("output/uid/districtWiseCount.csv")

  }

}