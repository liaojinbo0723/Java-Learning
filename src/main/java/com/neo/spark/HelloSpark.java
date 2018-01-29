package com.neo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.File;
import java.util.Arrays;
import java.util.List;
/**
 * 返回文本字符出现次数频率最高的前五
 */

public class HelloSpark {

    /**
     * 删除目录以及目录下的所有文件以及文件夹
     *
     * @return
     * @paramv_dir
     */

    public static Boolean deleteDir(String dir) {
        Boolean flag = false;
        File td = new File(dir);
        if (!td.exists()) {
            return false;
        }
        if (!td.isDirectory()) {
            return flag;
        }
        File[] files = td.listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isFile()) {
                f.delete();
            }
            if (f.isDirectory()) {
                deleteDir(f.toString());
            }
        }
        td.delete();
        flag = true;
        return flag;
    }

    public static void main(String[] args) {

        SparkConf sc = new SparkConf().setAppName("TestSpark").setMaster("local");
        String localPath = "C:/Users/xn043275/Desktop/xx.sql";
        String resPath = "C:/Users/xn043275/Desktop/spark_res";
        deleteDir(resPath);
        //实例JavaSparkContext对象
        JavaSparkContext jsc = new JavaSparkContext(sc);
        //读取本地文件
        JavaRDD <String> lines = jsc.textFile(localPath);
        //按空格拆分
        JavaRDD <String> words = lines.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        );
        //统计字符出现的次数
        JavaPairRDD<String,Integer> wordCnt = words.mapToPair(
                word -> new Tuple2<>(word, 1)).reduceByKey((x, y) -> x + y
        );
        //rdd反转key-value
        JavaPairRDD<Integer,String> wordCntReverse = wordCnt.mapToPair(
                wc -> new Tuple2<>(wc._2(),wc._1())
        );
        System.out.println("字符去重总数为:" + wordCnt.count());
        List<Tuple2<String,Integer>> wordCntList = wordCnt.collect();
        System.out.println("每个字符出现的次数如下:");
        wordCntList.forEach(s -> System.out.println("  " + s._1() + "出现的次数为:" + s._2()));
        //反转之后倒序排列取top5
        JavaPairRDD<Integer,String> wordCntTop =  wordCntReverse.sortByKey(false);
        List<Tuple2<Integer,String>> topList = wordCntTop.take(5);
        System.out.println("出现频率最高的五个字符如下:");
        topList.forEach(s -> System.out.println("  " + s._2() + "出现的次数为:" + s._1()));
        //转换RDD，将结果写入文件
        JavaPairRDD<String,Integer> topResult = jsc.parallelize(topList).mapToPair(
                s -> new Tuple2<>(s._2(),s._1())
        );
        topResult.saveAsTextFile(resPath);
        jsc.stop();
    }

}
