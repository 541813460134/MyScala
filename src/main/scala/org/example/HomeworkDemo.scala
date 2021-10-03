package org.example

import org.apache.spark.sql.{SQLContext, SparkSession}

object HomeworkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark").master("local[12]").getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext;
    val sqlContext = new SQLContext(sc)
    // header是否指定头部行作为schema。
    //推断数据类型
    val gradedf = sqlContext.read.format("csv").option("header","true").option("inferSchema", "true").load("D:/demo.csv")

    gradedf.show();

    println("输出每个同学的大学GPA") //
    gradedf.select("ID","collegeGPA").show()
    println()


    println("输出的大学GPA排名前10的同学") //
    gradedf.select("ID","collegeGPA").sort(gradedf("collegeGPA").desc).show(10)
    println()

    println("输出的大学GPA排名后10的同学") //
    gradedf.select("ID","collegeGPA").sort(gradedf("collegeGPA").asc).show(10)
    println()


    println("统计每个专业的平均绩点")
    gradedf.select("Specialization","collegeGPA").groupBy("Specialization").mean("collegeGPA").show()
    println()

    println("统计每个专业的绩点总和")
    gradedf.select("Specialization","collegeGPA").groupBy("Specialization").sum("collegeGPA").show()
    println()


    println("统计每个专业的最高绩点")
    gradedf.select("Specialization","collegeGPA").groupBy("Specialization").max("collegeGPA").show()
    println()


    println("统计每个专业的最低绩点")
    gradedf.select("Specialization","collegeGPA").groupBy("Specialization").min("collegeGPA").show()



    println("统计每个专业的平均薪资")
    gradedf.select("Specialization","Salary").groupBy("Specialization").mean("Salary").show()
    println()



    println("统计每个专业的最高薪资")
    gradedf.select("Specialization","Salary").groupBy("Specialization").max("Salary").show()
    println()


    println("统计每个专业的最低薪资")
    gradedf.select("Specialization","Salary").groupBy("Specialization").min("Salary").show()
    println()









   // println("输出当前死亡率排名前5的国家及死亡率；")
   // gradedf.select("国家","死亡率").sort(gradedf("死亡率").desc).show(5)


   // println("输出死亡人数排名前5的国家及死亡率；")
  //  gradedf.select("国家","死亡人数").sort(gradedf("死亡人数").desc).show(5)


   // println("输出累计确诊前5的国家及死亡率；")
   // gradedf.select("国家","累计确诊").sort(gradedf("累计确诊").desc).show(5)


   // println("输出当前治愈人数排名前5的国家及治愈人数；")
   // gradedf.select("国家","治愈人数").sort(gradedf("治愈人数").desc).show(5)



   // println("输出当前各洲确诊人数；")
    //gradedf.select("所在大洲","当前确诊").groupBy("所在大洲").sum().show()



    //println("输出各洲累计死亡人数；") //输出各洲累计死亡人数；
    //gradedf.select("死亡人数")
   // gradedf.select("所在大洲","死亡人数").groupBy("所在大洲").sum("死亡人数").show()



    //返回不含重复记录的数据
   // gradedf.distinct().show()




  }



}
