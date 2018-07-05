package com.neo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 获取占用存储较大的分区表清单，以便脚本进行压缩处理
 */
public class DataCleanTables {

   public static ArrayList<String> dbList = new ArrayList<String>(
           Arrays.asList("dwd_data", "ods_fq_cbs", "ods_fq_neo","ods_ph_eam","ods_ph_xndb","ods_ph_xndb3")
   );


    public static FileSystem getFileSystem() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://10.8.34.2:9000";
//        String hdfsPath = "hdfs://10.17.2.121:9000";
        conf.set("fs.defaultFS", hdfsPath);
        FileSystem fs = FileSystem.get(new URI(hdfsPath), conf, "hadoop");
        return fs;
    }



    public static void writeFile(File file, String tableName) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
            bw.write(tableName + "\n");
            bw.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getTables(File file) throws InterruptedException, IOException, URISyntaxException {
        FileSystem fs = getFileSystem();
        String rootPath = "/user/hive/warehouse/";
//        FileStatus[] status = fs.listStatus(new Path("/user/hive/warehouse/"));
//        String isMatch = ".+(ods|dwd|dmb).+db$";
        for(String list:dbList){
            FileStatus[] tabStats = fs.listStatus(new Path(rootPath + list + ".db"));
            for (FileStatus tab : tabStats) {
                    Path tabPath = tab.getPath();
                    FileStatus[] partStats = fs.listStatus(tabPath);
                    for (FileStatus part : partStats) {
                        String partPath = part.getPath().toString();
                        if (partPath.contains("part_dt=")) {
                            System.out.println(partPath);
                            Pattern tabMatch = Pattern.compile(".+warehouse/(.+)\\.db/(.+)/.+");
                            Matcher res = tabMatch.matcher(partPath);
                            String tableName;
                            if(res.find()){
                                tableName = res.group(1) + "." + res.group(2);
                                writeFile(file,tableName);
                                System.out.println("tableName:" + tableName);
                            }
                            break;
                        }
                    }
                }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        File file = new File("table_name.txt");
        if (file.exists()) {
            file.delete();
        }
        DataCleanTables.getTables(file);

    }
}

