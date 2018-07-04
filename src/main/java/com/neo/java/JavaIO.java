package com.neo.java;

import java.io.*;

public class JavaIO {

    public static void writeFile(File file) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(file,true));
            for (int i = 0; i < 10; i++) {
                bw.write(i + "\n");
            }
            bw.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readFile(File file){
        try {
            String str = null;
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            while(true){
                str = br.readLine();
                if(str == null){
                    break;
                }
                System.out.println(str);
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        File file = new File("xx.txt");
        if (file.exists()) {
            System.out.println("found a file!!!");
            writeFile(file);
            readFile(file);
        } else {
            System.out.println("not a file!!");
            writeFile(file);
            readFile(file);
        }
    }
}
