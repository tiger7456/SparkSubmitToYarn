package com.scanflove.sparksql

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object RemoteSubmitApp {
    val logger = Logger.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {

//        System.setProperty("HDP_VERSION", "3.1.4.0-315")
        // 设置提交任务的用户
        //    System.setProperty("HADOOP_USER_NAME", "root")
        val conf = new SparkConf().setAppName("Remote_Submit_App")
                // 设置yarn-client模式提交
                .setMaster("yarn-client") // 设置resourcemanager的ip
//                .set("yarn.resourcemanager.hostname", "hdp-master01")
                // 设置driver的内存大小
//                .set("spark.driver.memory", "256M")
                // 设置executor的内存大小
//                .set("spark.executor.memory", "256M")
                // 设置executor的个数
                .set("spark.executor.instance", "1")
                // 设置提交任务的 yarn 队列
                //      .set("spark.yarn.queue", "defalut")
                // 设置driver的 ip 地址,即本机的 ip 地址
                .set("spark.driver.host", "192.168.0.107")
                .set("mapred.input.dir.recursive", "true")
                .set("hive.mapred.supports.subdirectories", "true")
                // 设置序列化
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开
//                .setJars(List("D:\\IdeaProjects\\SparkSubmitToYarn\\target\\SparkSubmitToYarn-1.0.jar"))
//        val scc = new StreamingContext(conf, Seconds(30))
//
//        scc.sparkContext.setLogLevel("WARN")
//        //    scc.checkpoint("checkpoint")
//        val topic = "remote_submit_test"
//        val topicSet = topic.split(",").toSet

        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
//        spark.sql("use test");
        spark.sql("show databases").show()
        spark.sql("use sparkpractice")
        spark.sql("show tables").show()
        spark.sql("select * from city_info").show()

        //释放资源
        spark.stop()

    }
}
