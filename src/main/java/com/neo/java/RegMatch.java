package com.neo.java;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegMatch {

    List<String> joblist = new ArrayList<String>();

    /**
     * 递归取出job的绝对路径
     *
     * @param filePath
     * @return
     */
    public List<String> getFile(String filePath) {
        File file = new File(filePath);
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isFile()) {
                if (f.getName().contains(".kjb")) {
                    joblist.add(f.getAbsolutePath());
                }
            }
            if (f.isDirectory()) {
                getFile(f.getAbsolutePath());
            }
        }
        return joblist;
    }

    /**
     * * 读取job的内容
     *
     * @param filename
     * @return
     * @throws IOException
     */
    public static String getString(String filename) throws IOException {
        File file = new File(filename);
        FileReader reader = new FileReader(file);
        BufferedReader bReader = new BufferedReader(reader);
        StringBuilder sb = new StringBuilder();
        String s = "";
        while ((s = bReader.readLine()) != null) {
            sb.append(s + "\n");
        }
        bReader.close();
        return sb.toString().toLowerCase();
    }

    /**
     * 正则匹配from和join后的表名
     *
     * @param file
     * @throws IOException
     */
    public static void matchJob(String file) throws IOException {
        String str = "\\s+(from|join)\\s+(\\w+)[.](\\w+)";
        String sql = getString(file);
        Pattern p = null;
        String result = "";
        p = Pattern.compile(str);
        Matcher matcher = p.matcher(sql);
        if (file.contains("job_stg")) {
            System.out.println(file);
        } else if (file.contains("job_ods")) {
            result = new File(file).getName().replace("job_ods", "job_stg");
        } else {
            while (matcher.find()) {
                String string1 = "job_" + matcher.group(3);
                result = result + "@" + string1;
            }
        }
        System.out.println(new File(file).getName() + "----->" + result);
    }

    public static void main(String[] args) throws IOException {
        String filePath = "D:\\nbd\\002_scheduler\\02_kettle\\bi\\zx";
        List<String> resultList = (new RegMatch()).getFile(filePath);
        for (int i = 0; i < resultList.size(); i++) {
            matchJob(resultList.get(i));
        }
    }
}
