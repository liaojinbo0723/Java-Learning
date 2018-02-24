package com.neo.datamanager;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

public class RealTimeZxMagic {

    /**
     * 获取配置文件信息
     *
     * @return
     */
    private static Properties getConfigInfo() {
        InputStream is = RealTimeJobMonitor.class.getResourceAsStream("/config2.properties");

        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    private void checkRealTimeData() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String cur_date = sdf.format(new Date());
        Calendar cal = Calendar.getInstance();
        int hour = cal.get(Calendar.HOUR_OF_DAY) - 1;
        ConnMysql fc = new ConnMysql();
        Properties prop = getConfigInfo();
        String dbUrl = prop.getProperty("zx_jdbc_url");
        String dbUser = prop.getProperty("zx_jdbc_username");
        String dbPass = prop.getProperty("zx_jdbc_password");
        Connection conndb = fc.connMysql(dbUrl, dbUser, dbPass);
        String sql = "select round(sum(t.inv_amt),0)  from dm.dwd_evt_zx_achv_rltm_stat_per_hour t " +
                "where t.dat_dt = curdate()" +
                "and t.dat_dt_hour_dist=" + hour;
        System.out.println(sql);
        ResultSet rsHour = fc.querySql(sql, conndb);
        String daySql = "select count(0) from dm.dwd_evt_zx_achv_rltm_stat_per_day t " +
                "where t.dat_dt = curdate()";
        ResultSet rsDay = fc.querySql(daySql,conndb);
        try {
            if (rsHour.next()) {
                String resHour = rsHour.getString(1);
                if (Integer.parseInt(resHour) <= 0) {
                    System.out.println(resHour);
                    phoneAlarm();
                }else{
                    System.out.println(cur_date + " table:per_hour 数据正常!!!");
                }
            }
            if (rsDay.next()){
                String resDay = rsDay.getString(1);
                if (Integer.parseInt(resDay) <= 0) {
                    phoneAlarm();
                }else{
                    System.out.println(cur_date + " table:per_day 数据正常!!!");
                }
            }
            rsHour.close();
            rsDay.close();
            conndb.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 预警
     */
    private static void phoneAlarm() {
        Properties prop = getConfigInfo();
        String mobile = prop.getProperty("mobile");
        String[] mobilelist = mobile.split(",");
        String message = prop.getProperty("message2");
        for (int i = 0; i < mobilelist.length; i++) {
            String command = "sh /appcom/script/alarm_interface/voice_monit.sh " +
                    mobilelist[i] + " " + message;
            try {
                int res = Runtime.getRuntime().exec(command).waitFor();
                if (res != 0) {
                    System.out.println("the command exec failed!! status:" + res);
                    System.exit(1);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
//        RealTimeZxMagic zxMagic = new RealTimeZxMagic();
//        zxMagic.checkRealTimeData();
        Calendar cal = Calendar.getInstance();
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        System.out.println(hour);
    }
}
