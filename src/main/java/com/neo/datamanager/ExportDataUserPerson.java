package com.neo.datamanager;

import com.neo.xnol.security.service.DecryptService;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

public class ExportDataUserPerson {

    /**
     * 获取配置文件信息
     *
     * @return
     */
    private static Properties getConfigInfo() {
        InputStream is = RealTimeJobMonitor.class.getResourceAsStream("/config.properties");

        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }


    /**
     * 获取数据
     */
    private void getData(String strDate) {
        ConnMysql fc = new ConnMysql();
        String dbJdbc = getConfigInfo().getProperty("zxprod_jdbc_url");
        String dbUser = getConfigInfo().getProperty("zxprod_jdbc_username");
        String dbPass = getConfigInfo().getProperty("zxprod_jdbc_password");
        Connection conn = fc.connMysql(dbJdbc, dbUser, dbPass);
        String sql = "select id,\n" +
                "ifnull(username,''),\n" +
                "ifnull(userType,''),\n" +
                "ifnull(userNo,''),\n" +
                "ifnull(realName,''),\n" +
                "ifnull(gender,''),\n" +
                "ifnull(birthdate,''),\n" +
                "ifnull(email,''),\n" +
                "ifnull(mobile,''),\n" +
                "ifnull(idCardNo,''),\n" +
                "ifnull(idCardType,''),\n" +
                "ifnull(loginPwd,''),\n" +
                "ifnull(paymentPwd,''),\n" +
                "ifnull(status,''),\n" +
                "ifnull(securityLevel,''),\n" +
                "ifnull(elecSign,''),\n" +
                "ifnull(createTime,''),\n" +
                "ifnull(lstModTime,''),\n" +
                "ifnull(syncCreateTime,''),\n" +
                "ifnull(syncUpdateTime,''),\n" +
                "ifnull(digest,'') from xnaccount.t_user_person where createTime >= '" + strDate + "'";
        System.out.println(MessageFormat.format("sql:{0}",sql));
        ResultSet rs = fc.querySql(sql, conn);
        int count = 1;
        try {
            while (rs.next()) {
                TableUserPerson userData = new TableUserPerson();
                userData.setId(rs.getString(1));
                userData.setUsername(rs.getString(2));
                userData.setUserType(rs.getString(3));
                userData.setUserNo(rs.getString(4));
                if(rs.getString(5).equals("")){
                    userData.setRealName("");
                }else{
                    userData.setRealName(DecryptService.decryptData(rs.getString(5)));
                }

                userData.setGender(rs.getString(6));
                userData.setBirthdate(rs.getString(7));

                if(rs.getString(8).equals("")){
                    userData.setEmail("");
                }else{
                    userData.setEmail(DecryptService.decryptData(rs.getString(8)));
                }

                if(rs.getString(9).equals("")){
                    userData.setMobile("");
                }else{
                    userData.setMobile(DecryptService.decryptData(rs.getString(9)));
                }

                if(rs.getString(10).equals("")){
                    userData.setIdCardNo("");
                }else{
                    userData.setIdCardNo(DecryptService.decryptIdNo(rs.getString(10)));
                }
                userData.setIdCardType(rs.getString(11));
                userData.setLoginPwd(rs.getString(12));
                userData.setPaymentPwd(rs.getString(13));
                userData.setStatus(rs.getString(14));
                userData.setSecurityLevel(rs.getString(15));
                userData.setElecSign(rs.getString(16));
                userData.setCreateTime(rs.getString(17));
                userData.setLstModTime(rs.getString(18));
                userData.setSyncCreateTime(rs.getString(19));
                userData.setSyncUpdateTime(rs.getString(20));
                userData.setDigest(rs.getString(21));
                System.out.println(userData.toString());
                insertData(userData.toString());
                System.out.println(MessageFormat.format("第{0}条数据已插入",count));
                count++;
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void truncateTable(){
        ConnMysql fc = new ConnMysql();
        String dbJdbc = getConfigInfo().getProperty("zx_jdbc_url");
        String dbUser = getConfigInfo().getProperty("zx_jdbc_username");
        String dbPass = getConfigInfo().getProperty("zx_jdbc_password");
        Connection conn = fc.connMysql(dbJdbc, dbUser, dbPass);
        String trunSql = "truncate table adw.t_user_person_temp";
        fc.dmlSql(trunSql,conn);
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    /**
     * insert to table
     * @param sql
     */
    private void insertData(String sql) throws SQLException {
        ConnMysql fc = new ConnMysql();
        String dbJdbc = getConfigInfo().getProperty("zx_jdbc_url");
        String dbUser = getConfigInfo().getProperty("zx_jdbc_username");
        String dbPass = getConfigInfo().getProperty("zx_jdbc_password");
        Connection conn = fc.connMysql(dbJdbc, dbUser, dbPass);
        String  insertSql = "insert into adw.t_user_person_temp " + sql;
        fc.dmlSql(insertSql,conn);
        conn.close();
    }

    public static void main(String[] args) {
        String startDate = args[0];
        ExportDataUserPerson expData = new ExportDataUserPerson();
        truncateTable();
        expData.getData(startDate);

    }

}
