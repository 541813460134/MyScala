package org.example

import org.apache.spark
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/**
 * @autor 王启蒙
 * @create 2021/6/7 20:28
 */
object ShixunDem {

  def main(args: Array[String]): Unit = {
    //一个StruceField你可以把它当成一个特征列。分别用列的名称和数据类型初始化
    val structFields = List(StructField("leixing",StringType),StructField("nianfen",IntegerType),
      StructField("licheng",DoubleType),StructField("didian",StringType),
      StructField("shoujia",DoubleType),StructField("yuanjia",DoubleType))
    //最后通过StructField的集合来初始化表的模式。
    val types = StructType(structFields)

    val sparkConf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    sparkContext.setLogLevel("ERROR")
    val rdd = sparkContext.textFile("D:/guazi.csv")
    //Rdd的数据，里面的数据类型要和之前的StructField里面数据类型对应。否则会报错。
    val rowRdd = rdd.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }.map(line=>Row(line.trim.split(",")(0).toString,line.trim.split(",")(1).replace("年","").toInt
      ,line.trim.split(",")(2).toString.replace("万公里","").toDouble,line.trim.split(",")(3).toString,line.trim.split(",")(4).toString.replace("万","").replace("补贴后","").toDouble
      ,line.trim.split(",")(5).toString.replace("万","").toDouble))


    //通过SQLContext来创建DataFrame.
    val gradedf = sqlContext.createDataFrame(rowRdd,types)






    println("输出年份为2015年的信息")
    gradedf.filter(gradedf("nianfen").===(2015)).show()
    println()

    println("里程跑的最多的前10名")
    gradedf.select("leixing","licheng").sort(gradedf("licheng").desc).show(10);
    println()





    println("里程跑的最少的前10名")
    gradedf.select("leixing","licheng").sort(gradedf("licheng").asc).show(10);
    println()

    println("输出原价排名前10的车")
    gradedf.select("leixing","yuanjia").sort(gradedf("yuanjia").desc).show(10);
    println()

    println("输出原价排名后10的车")
    gradedf.select("leixing","yuanjia").sort(gradedf("yuanjia").asc).show(10);
    println()



    println("输出售价排名前10的车")
    gradedf.select("leixing","shoujia").sort(gradedf("shoujia").desc).show(10);
    println()

    println("输出售价排名后10的车")
    gradedf.select("leixing","shoujia").sort(gradedf("shoujia").asc).show(10);
    println()


    println("输出年份为2014年，售价为7万的车")//输出年份为2014年，售价为7万的车
    gradedf.where("nianfen='2014' and shoujia = '7.00'").select("leixing","shoujia","nianfen").show()
    println()


    println("输出年份为2012年，售价为10万的车")//输出年份为2012年，售价为10万的车
    gradedf.where("nianfen='2012' and shoujia = '10.00'").select("leixing","shoujia","nianfen").show()
    println()


    println("每种车的数量")//每种车的数量
    gradedf.select("leixing").groupBy("leixing").count().show()
    println()





    println("去掉重复年份的信息")// drop() 去除重复字段  去掉重复年份
    gradedf .selectExpr("leixing" , "shoujia" , " nianfen" ).drop("nianfen").show()
    println()




    println("输出年份出现概率大于0.5的年份")//年份出现概率大于0.5的年份
    gradedf.stat.freqItems(Seq ("nianfen") , 0.5).show()
    println()


    println("输出售价出现概率大于0.5的年份")
    gradedf.stat.freqItems(Seq ("shoujia") , 0.5).show()
    println()



    println("替换shoujia列为售价 leixing列为类型")
    gradedf.withColumnRenamed( "shoujia" , "售价" ).withColumnRenamed( "leixing" , "类型" ).select("类型","售价").show()
    println()


    println("不同里程的最大售价")
    gradedf.select("leixing","licheng","shoujia").groupBy("licheng").max("shoujia").show()
    println()

    println("不同年份的最大售价")
    gradedf.select("nianfen","licheng","shoujia").groupBy("nianfen").max("shoujia").show()
    println()





  }

}
