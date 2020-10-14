package com.scanflove.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LocalApp {
    def main(args: Array[String]): Unit = {
        //创建配置文件对象
//        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_LocalApp")
        val conf: SparkConf = new SparkConf().setMaster("yarn-client").setAppName("SparkSQL_LocalApp")
        //创建SparkSession对象
        val spark: SparkSession = SparkSession.builder()
                .config("mapred.input.dir.recursive", "true")
                .config("hive.mapred.supports.subdirectories", "true")
                .config("spark.driver.host", "192.168.0.107")
                .enableHiveSupport().config(conf).getOrCreate()

        spark.sql("show databases").show()
        spark.sql("use sparkpractice")
        spark.sql("show tables").show()
        spark.sql("select * from city_info").show()
//        Thread.sleep(1000000)
        //释放资源
        spark.stop()

    }


}
