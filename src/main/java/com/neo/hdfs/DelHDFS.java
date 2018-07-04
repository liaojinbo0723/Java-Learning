package com.neo.hdfs;

import com.neo.datamanager.DateOper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * java api操作hdfs
 */
public class DelHDFS {

    public static final Long lastYear = DateOper.strToDate("2017-07-04").getTime();


    public static FileSystem getFileSystem() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://10.8.34.2:9000";
//        String hdfsPath = "hdfs://10.17.2.121:9000";
        conf.set("fs.defaultFS", hdfsPath);
        FileSystem fs = FileSystem.get(new URI(hdfsPath), conf, "hadoop");
        return fs;
    }


    public static void writeFile(File file, String path) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
            bw.write(path + "\n");
            bw.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void listAllFiles(File file) throws IOException, URISyntaxException, InterruptedException {
        FileSystem fs = getFileSystem();
        FileStatus[] status = fs.listStatus(new Path("/user/hive/warehouse"));
        String isMatch = ".+(ods|dwd|dmb).+db$";
//        String isMatch = ".+dwd.+db$";
        long space = 0;
        for (FileStatus st : status) {
            if (Pattern.matches(isMatch, st.getPath().toString())) {
                Path dbPath = st.getPath();
                FileStatus[] tabStats = fs.listStatus(dbPath);
                for (FileStatus tab : tabStats) {
                    Path tabPath = tab.getPath();
                    FileStatus[] partStats = fs.listStatus(tabPath);
                    for (FileStatus part : partStats) {
                        String partPath = part.getPath().toString();
                        if (partPath.contains("part_dt=")) {
                            //正则匹配分区日期
                            Pattern p = Pattern.compile(".+part_dt=(.+)");
                            Matcher res = p.matcher(partPath);
                            if (res.find()) {
                                String part_dt = res.group(1);
                                String dateMatch = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
                                if (Pattern.matches(dateMatch, part_dt)) {
                                    //判断分区是否是一年前且不是月末，是则删除
                                    if (DateOper.strToDate(part_dt).getTime() < lastYear && !part_dt.equals(DateOper.getEndDateOfMonth(part_dt))) {
                                        System.out.println(partPath + "    " + fs.getContentSummary(part.getPath()).getLength());
                                        writeFile(file, partPath);
                                        space += fs.getContentSummary(part.getPath()).getLength();
                                    }

                                } else {
                                    System.out.println("partition is not a date:" + partPath);
                                }


                            }
                        }

                    }
                }
            }
        }
        System.out.println("删除的总空间大小为:" + space/1024/1024/1024/1024 + "T");
        fs.close();
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        File file = new File("res.txt");
        if (file.exists()) {
            file.delete();
        }
        DelHDFS.listAllFiles(file);

    }
}
