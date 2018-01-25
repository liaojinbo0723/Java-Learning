package com.neo.datamanager;



import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;



/**
 * 实现功能：电销线下名单引流人和销售人保证不一致
 * 采用随机分布实现
 *
 */
public class ForRandomList {

    public final static int DISTRIBUTE = 30;

    /**
     * 获取配置文件信息
     * @return
     */
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

    /**
     *
     * 随机list直到引流人和销售人不一致为止
     */
    private void getResList(List listOrg,List listRes){
        Collections.shuffle(listRes);
        boolean flag = true;
        int cnt = 0;
        int j = 0;
        while (flag){
            j++;

            for (int i = 0; i < listOrg.size(); i++) {
                if (!listOrg.get(i).equals(listRes.get(i))){
                    cnt++;
                }else {
                    cnt = 0;
                    Collections.shuffle(listRes);
                    break;
                }
                if (cnt == listOrg.size()){
                    flag = false;
                }
            }
        }
        System.out.println("执行的次数为：" + j);
        System.out.println(listOrg);
        System.out.println(listRes);
    }

    /**
     * 获取 名单id 和 引流人的集合
     * @return
     */
    private void getListInit(List list1,List list2,List list3) {

        ForConnMysql fc = new ForConnMysql();
        Properties prop = getConfigInfo();
        String dburl = prop.getProperty("zx_jdbc_url");
        String dbuser = prop.getProperty("zx_jdbc_username");
        String dbpass = prop.getProperty("zx_jdbc_password");
        Connection conndb = fc.connMysql(dburl,dbuser,dbpass);
        String v_sql = "select t.list_id,t.old_caller from dm.rpt_tm_call_list_new_prod t where caller is null; ";
        ResultSet rs = fc.querySql(v_sql,conndb);
        try {
            while (rs.next()){
                list1.add(rs.getString(1));
                list2.add(rs.getString(2));
                list3.add(rs.getString(2));
            }
            rs.close();
            conndb.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void delListid(List listTmp){
        String strList = String.join(",",listTmp);
        ForConnMysql fc = new ForConnMysql();
        Properties prop = getConfigInfo();
        String dburl = prop.getProperty("zx_jdbc_url");
        String dbuser = prop.getProperty("zx_jdbc_username");
        String dbpass = prop.getProperty("zx_jdbc_password");
        Connection conndb = fc.connMysql(dburl,dbuser,dbpass);
        String sql = "delete from dm.rpt_tm_call_list_new_prod where list_id in (" + strList + ")";
        System.out.println(sql);
        if(fc.dmlSql(sql,conndb)){
            System.out.println("delete suceessful! list_id:" + strList);
        }

        try {
            conndb.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

      private void updateListid(List list1,List list2){
        String str_list = String.join(",",list1);
        ForConnMysql fc = new ForConnMysql();
        Properties prop = getConfigInfo();
        String dburl = prop.getProperty("zx_jdbc_url");
        String dbuser = prop.getProperty("zx_jdbc_username");
        String dbpass = prop.getProperty("zx_jdbc_password");
        Connection conndb = fc.connMysql(dburl,dbuser,dbpass);
        for (int i = 0; i < list1.size(); i++) {
            String sql = "update dm.rpt_tm_call_list_new_prod set caller = '" + list2.get(i) +
                    "' where caller is null and list_id = " + list1.get(i);
            if(fc.dmlSql(sql,conndb)){
                System.out.println("list_id:" + list1.get(i) + " update suceessful!");
            }
        }
        String v_account = " update dm.rpt_tm_call_list_new_prod t" +
                " inner join dm.dim_employee_distribute d" +
                " on t.caller = d.Employee_Name" +
                " set t.account_name = d.account_name" +
                " where t.account_name is null " +
                " and t.list_id in (" + str_list + ")";
        if(fc.dmlSql(v_account,conndb)){
                System.out.println("account_name update suceessful!");
            }
        try {
            conndb.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Boolean randomFlag(List listid) {
        String str_list = String.join(",",listid);
        ForConnMysql fc = new ForConnMysql();
        Properties prop = getConfigInfo();
        String dburl = prop.getProperty("zx_jdbc_url");
        String dbuser = prop.getProperty("zx_jdbc_username");
        String dbpass = prop.getProperty("zx_jdbc_password");
        Connection conndb = fc.connMysql(dburl,dbuser,dbpass);
        String sql = "select sum(cnt) as cnt_tot,max(cnt) as cnt_max " +
                "  from (select old_caller, count(0) as cnt " +
                "  from dm.rpt_tm_call_list_new_prod" +
                "  where caller is null and list_id in (" + str_list +
                "  ) group by old_caller) x";
        ResultSet rs = fc.querySql(sql,conndb);
        Integer cntTot = 0;
        Integer cntMax = 0;
        try {
            while (rs.next()){
                cntTot = Integer.parseInt(rs.getString(1));
                cntMax = Integer.parseInt(rs.getString(2));
            }
            if (cntTot == 0){
                return false;
            }
            if(cntMax > (cntTot/2)){
                return false;
            }else {
                return true;
            }
        } catch (SQLException e) {
            return false;
        }
    }

    public static void main(String[] args) {
        //定义三个list,存放list_id,list_oldcaller,list_caller
        List<String> listId=new ArrayList<String>();
        List<String> listOrg=new ArrayList<String>();
        List<String> listRes=new ArrayList<String>();

        ForRandomList fr = new ForRandomList();
        //获取三个list
        fr.getListInit(listId,listOrg,listRes);
        int loop_cnt = 0;
        if (listId.size()%DISTRIBUTE==0){
            loop_cnt = (int)Math.floor(listId.size()/DISTRIBUTE);
        }else {
            loop_cnt = (int)Math.floor(listId.size()/DISTRIBUTE) + 1;
        }

        System.out.println("名单总数为:" + listId.size() + "循环次数为:" + loop_cnt);
        for (int i = 0; i < loop_cnt; i++) {
            if(i == loop_cnt - 1){
                List<String> listIdTmp=new ArrayList<String>();
                List<String> listOrgTmp=new ArrayList<String>();
                List<String> listResTmp=new ArrayList<String>();
                for (int j = i*30; j < listId.size(); j++) {
                    listIdTmp.add(listId.get(j));
                    listOrgTmp.add(listOrg.get(j));
                    listResTmp.add(listOrg.get(j));
                }
                if(fr.randomFlag(listIdTmp)){
                    fr.getResList(listOrgTmp,listResTmp);
                    fr.updateListid(listIdTmp,listResTmp);
                    System.out.println("第" + (i+1) + "次分发成功!" );
                }else {
                    fr.delListid(listIdTmp);
                    System.out.println("第" + (i+1) + "次分发失败!" );
                }
            }else{
                List<String> listIdTmp=new ArrayList<String>();
                List<String> listOrgTmp=new ArrayList<String>();
                List<String> listResTmp=new ArrayList<String>();
                for (int j = i*30; j < i*30 + DISTRIBUTE; j++) {
                    listIdTmp.add(listId.get(j));
                    listOrgTmp.add(listOrg.get(j));
                    listResTmp.add(listOrg.get(j));
                }
                if(fr.randomFlag(listIdTmp)){
                    fr.getResList(listOrgTmp,listResTmp);
                    fr.updateListid(listIdTmp,listResTmp);
                    System.out.println("第" + (i+1) + "次分发成功!" );
                }else {
                    fr.delListid(listIdTmp);
                    System.out.println("第" + (i+1) + "次分发失败!" );
                }

            }
        }
    }
}