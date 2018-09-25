package com.neo.java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author xn043275
 * @Description:
 * @date 2018/9/25
 */
public class JavaMysqlToHiveCreTab {

    public static final Logger logger = LoggerFactory.getLogger(JavaMysqlToHiveCreTab.class);

    /**
     * 读取jar包外的配置文件
     * @return
     */
    private static Properties getConfigInfoOut() {
        String filePath = System.getProperty("user.dir")  + "\\config3.properties";
        logger.info("读取配置文件路径为:" + filePath);
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
        System.out.println(getConfigInfoOut().getProperty("dev_jdbc_password"));
    }
}
