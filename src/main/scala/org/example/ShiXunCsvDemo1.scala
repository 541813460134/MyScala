package org.example

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions
object ShiXunCsvDemo1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark json").master("local[2]").getOrCreate();
    val sc = spark.sparkContext;
    spark.sparkContext.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    // header是否指定头部行作为schema。
    val gradedf = sqlContext.read.format("csv").option("header","true").option("inferSchema", "true").load("D:/guazi.csv")

    //gradedf.show();

    println("数据共有"+gradedf.select().count()+"条数据")

    println("输出年份为2015年的信息")
    gradedf.where("nianfen='2015年'").show()
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
    gradedf.where("nianfen='2014年' and shoujia = '7.00万'").select("leixing","shoujia","nianfen").show()
    println()


    println("输出年份为2012年，售价为10万的车")//输出年份为2012年，售价为10万的车
    gradedf.where("nianfen='2012年' and shoujia = '10.00万'").select("leixing","shoujia","nianfen").show()
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


  }

}
