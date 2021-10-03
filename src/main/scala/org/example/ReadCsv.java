package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class ReadCsv {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("读取csv做统计").master("local[*]").getOrCreate();
        List<StructField> fs=new ArrayList<StructField>();
        StructField f1= DataTypes.createStructField("leixing", DataTypes.StringType, true);
        StructField f2=DataTypes.createStructField("nianfen", DataTypes.StringType, true);
        StructField f3=DataTypes.createStructField("licheng", DataTypes.StringType, true);
        StructField f4= DataTypes.createStructField("didian", DataTypes.StringType, true);
        StructField f5=DataTypes.createStructField("shoujia", DataTypes.StringType, true);
        StructField f6=DataTypes.createStructField("yuanjia", DataTypes.StringType, true);

        fs.add(f1);
        fs.add(f2);
        fs.add(f3);
        fs.add(f4);
        fs.add(f5);
        fs.add(f6);

        StructType schema=DataTypes.createStructType(fs);

		/*Dataset<Row> ds=spark.read()
		  //自动推断列类型
		  .option("inferSchema", "true")
		  //指定一个指示空值的字符串
		  .option("nullvalue", "?")
		  //当设置为 true 时，第一行文件将被用来命名列，而不包含在数据中
		  .option("header", "true")
		  .csv("/home/cry/myStudyData/userList.csv");*/

        Dataset<Row> ds=spark.read().schema(schema).csv("D:/guazi.csv");
        ds.createOrReplaceTempView("user");
        Dataset<Row> res=spark.sql("select * from user where nianfen = 2015年");
        res.show();

       // spark.stop();
    }
}
