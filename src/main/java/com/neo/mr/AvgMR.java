package com.neo.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class AvgMR {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, "\n");
            while (tokenizer.hasMoreElements()) {
                StringTokenizer strToken = new StringTokenizer(tokenizer.nextToken());
                String strName = strToken.nextToken();//姓名
                String strScore = strToken.nextToken();//成绩
                Text text = new Text(strName);
                IntWritable score = new IntWritable(Integer.parseInt(strScore));
                context.write(text, score);

            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int cnt = 0;
            for (IntWritable val : values) {
                sum += val.get();
                cnt++;
            }
            int avg = (int)sum/cnt;
            context.write(key, new IntWritable(avg));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mapreduce-avg");
        job.setJarByClass(AvgMR.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input/"));
        FileOutputFormat.setOutputPath(job, new Path("out2/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
