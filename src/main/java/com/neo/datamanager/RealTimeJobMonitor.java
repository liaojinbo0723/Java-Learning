package com.neo.datamanager;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * 监控在线准实时跑的作业，十五分钟作业没跑完就打电话告警
 * 打完电话之后更新作业状态
 */
public class RealTimeJobMonitor {


    private void getJobname(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String cur_date = sdf.format(new Date());
        For_ConnMysql fc = new For_ConnMysql();
        Properties prop = getConfigInfo();
        String db_url = prop.getProperty("mss_jdbc_url");
        String db_user = prop.getProperty("mss_jdbc_username");
        String db_pass = prop.getProperty("mss_jdbc_password");
        Connection conn_db = fc.conn_Mysql(db_url,db_user,db_pass);
        String v_sql = "select t.job_name,t.start_time,a.job_id,t.record_id from mss.run_job_record t " +
                " inner join mss.cm_job a " +
                " on t.job_name = a.JOB_NAME and a.IS_ENABLE = 1 " +
                " inner join mss.cm_group b " +
                " on a.GROUP_ID = b.GROUP_ID and b.G_NAME in ('stg_zx99_realtime','dwd_zx_pub_realtime','dwd_zx_realtime','dwd_zx_realtime_rpt') " +
                " where t.start_time>=curdate() " +
                " and t.status = 0";
        ResultSet rs = fc.query_Sql(v_sql,conn_db);
        int timeout_flag = 0;
        try {
            while (rs.next()){
                String str_jobname = rs.getString(1);
                String str_start = rs.getString(2);
                String str_jobid = rs.getString(3);
                String str_recordid = rs.getString(4);
                Date dt_start = sdf.parse(str_start);
                Date dt = new Date();
                Long sec = (dt.getTime() - dt_start.getTime())/1000;
                if (sec >= 900){
                    timeout_flag = 1;
                    System.out.println(cur_date + "  job_name:" + str_jobname + "执行超时!!");
                    phoneAlarm();
                    String v_update_sts = "update mss.cm_run_job set state = 3 where job_id = '" + str_jobid
                    + "' and state = 2";
                    String v_update_log = "update mss.run_job_record set status = 2 where record_id = '" + str_recordid
                    + "' and status = 0";
                    fc.dml_Sql(v_update_log,conn_db);
                    fc.dml_Sql(v_update_sts,conn_db);
                    System.out.println(cur_date + "  job_name:" + str_jobname + "状态更新成功");
                }

            }
            if (timeout_flag == 0 ){
                System.out.println(cur_date + "  小牛在线实时作业执行正常!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private Properties getConfigInfo(){
        InputStream is = RealTimeJobMonitor.class.getResourceAsStream("/config.properties");

        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    private void phoneAlarm(){
        Properties prop = getConfigInfo();
        String mobile = prop.getProperty("mobile");
        String[] mobilelist= mobile.split(",");
        String message = prop.getProperty("message");
        for (int i = 0; i < mobilelist.length; i++) {
            String command = "sh /appcom/script/alarm_interface/voice_monit.sh " +
                    mobilelist[i] + " " + message;
        try {
            int res = Runtime.getRuntime().exec(command).waitFor();
            if (res != 0){
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

        RealTimeJobMonitor rjm = new RealTimeJobMonitor();
        rjm.getJobname();

    }
}
