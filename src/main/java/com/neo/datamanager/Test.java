package com.neo.datamanager;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Test  {


    /**
     * 读取jar包内的配置文件
     * @return
     */
    private static Properties getConfigInfoIn() {
        InputStream is = Test.class.getResourceAsStream("/config2.properties");
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    /**
     * 读取jar包外的配置文件
     * @return
     */
    private static Properties getConfigInfoOut() {
        String filePath = System.getProperty("user.dir")  + "\\config3.properties";
        System.out.println(filePath);
        InputStream is = null;
        Properties prop = new Properties();
        try {
            is = new FileInputStream(filePath);
            prop.load(is);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    public static void main(String[] args) {
        Properties prop = getConfigInfoOut();
        System.out.println(prop.getProperty("user"));
    }
}
