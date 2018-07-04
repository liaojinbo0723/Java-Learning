package com.neo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingDemo {
    public static void main(String[] args) throws InterruptedException {
        //注意本地调试，master必须为local[n],n>1,表示一个线程接收数据，n-1个线程处理数据
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("streaming word count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //设置日志运行级别
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
        //创建一个将要连接到hostname:port 的离散流
//        JavaReceiverInputDStream<String> lines =
//                ssc.socketTextStream("master1", 9999);
        JavaReceiverInputDStream<String> lines = null;
        JavaPairDStream<String, Integer> counts =
                lines.flatMap(x -> Arrays.asList(x.split("\t")).iterator())
                        .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                        .reduceByKey((x, y) -> x + y);

        // 在控制台打印出在这个离散流（DStream）中生成的每个 RDD 的前十个元素
        counts.print();
        // 启动计算
        ssc.start();
        ssc.awaitTermination();
    }
}
