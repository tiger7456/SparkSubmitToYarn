package com.scanflove.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LocalApp {
    def main(args: Array[String]): Unit = {
        //创建配置文件对象
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_MySQL")
        //创建SparkSession对象
        val spark: SparkSession = SparkSession.builder()
                .config("mapred.input.dir.recursive", "true")
                .config("hive.mapred.supports.subdirectories", "true")
                .enableHiveSupport().config(conf).getOrCreate()

        spark.sql("show databases").show()
        spark.sql("use sparkpractice")
        spark.sql("show tables").show()
        spark.sql("select * from city_info").show()
        //释放资源
        spark.stop()
    }


}
