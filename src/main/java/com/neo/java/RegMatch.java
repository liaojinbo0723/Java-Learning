package com.neo.java;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegMatch {

    public static List<String> getFile(String filePath){
        File file = new File(filePath);
        List<String> list = new ArrayList();
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isFile()){
                if (f.getName().contains(".kjb")){
                    list.add(f.getAbsolutePath());
                }
            }
            if(f.isDirectory()){
                getFile(f.getAbsolutePath());
            }
        }
        return list;
    }

    public static String getString(String filename) throws IOException {
        File file = new File(filename);
        FileReader reader = new FileReader(file);
        BufferedReader bReader = new BufferedReader(reader);
        StringBuilder sb = new StringBuilder();
        String s = "";
        while ((s =bReader.readLine()) != null) {
            sb.append(s + "\n");
        }
        bReader.close();
        return sb.toString().toLowerCase();

    }
    public static void matchJob(String file) throws IOException {
        List<String> list = new ArrayList();
        list.add("\\s+from\\s+(\\w+)[.](\\w+)");
        list.add("\\s+join\\s+(\\w+)[.](\\w+)");
        String sql = getString(file);
        Pattern p=null;
        String result = "";
        for (int i = 0; i < list.size(); i++) {
            p = Pattern.compile(list.get(i));
            Matcher matcher=p.matcher(sql);
            while (matcher.find()) {
                String string1 ="job_" + matcher.group(2);
                result = result + "@" + string1;
            }
        }
        System.out.println(new File(file).getName() + ":" + result);
    }

    public static void main(String[] args) throws IOException {
        List<String> fileList = getFile("F:\\ETL");
        for (int i = 0; i < fileList.size(); i++) {
            matchJob(fileList.get(i));
        }

    }
}
