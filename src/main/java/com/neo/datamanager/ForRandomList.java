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
     * @param list_org  原始的名单list
     * @param list_res  随机之后的名单list
     */
    private void getResList(List list_org,List list_res){
        Collections.shuffle(list_res);
        boolean flag = true;
        int cnt = 0;
        int j = 0;
        while (flag){
            j++;

            for (int i = 0; i < list_org.size(); i++) {
                if (!list_org.get(i).equals(list_res.get(i))){
                    cnt++;
                }else {
                    cnt = 0;
                    Collections.shuffle(list_res);
                    break;
                }
                if (cnt == list_org.size()){
                    flag = false;
                }
            }
        }
        System.out.println("执行的次数为：" + j);
        System.out.println(list_org);
        System.out.println(list_res);
    }

    /**
     * 获取 名单id 和 引流人的集合
     * @return
     */
    private void getListInit(List list_a,List list_b,List list_c) {

        For_ConnMysql fc = new For_ConnMysql();
        Properties prop = getConfigInfo();
        String db_url = prop.getProperty("zx_jdbc_url");
        String db_user = prop.getProperty("zx_jdbc_username");
        String db_pass = prop.getProperty("zx_jdbc_password");
        Connection conn_db = fc.conn_Mysql(db_url,db_user,db_pass);
        String v_sql = "select t.list_id,t.old_caller from dm.rpt_tm_call_list_new_prod t where caller is null; ";
        ResultSet rs = fc.query_Sql(v_sql,conn_db);
        try {
            while (rs.next()){
                list_a.add(rs.getString(1));
                list_b.add(rs.getString(2));
                list_c.add(rs.getString(2));
            }
            rs.close();
            conn_db.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void delListid(List list_a){
        String str_list = String.join(",",list_a);
        For_ConnMysql fc = new For_ConnMysql();
        Properties prop = getConfigInfo();
        String db_url = prop.getProperty("zx_jdbc_url");
        String db_user = prop.getProperty("zx_jdbc_username");
        String db_pass = prop.getProperty("zx_jdbc_password");
        Connection conn_db = fc.conn_Mysql(db_url,db_user,db_pass);
        String v_sql = "delete from dm.rpt_tm_call_list_new_prod where list_id in (" + str_list + ")";
        System.out.println(v_sql);
        if(fc.dml_Sql(v_sql,conn_db)){
            System.out.println("delete suceessful! list_id:" + str_list);
        }

        try {
            conn_db.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

      private void updateListid(List list_a,List list_b){
        String str_list = String.join(",",list_a);
        For_ConnMysql fc = new For_ConnMysql();
        Properties prop = getConfigInfo();
        String db_url = prop.getProperty("zx_jdbc_url");
        String db_user = prop.getProperty("zx_jdbc_username");
        String db_pass = prop.getProperty("zx_jdbc_password");
        Connection conn_db = fc.conn_Mysql(db_url,db_user,db_pass);
        for (int i = 0; i < list_a.size(); i++) {
            String v_sql = "update dm.rpt_tm_call_list_new_prod set caller = '" + list_b.get(i) +
                    "' where caller is null and list_id = " + list_a.get(i);
            if(fc.dml_Sql(v_sql,conn_db)){
                System.out.println("list_id:" + list_a.get(i) + " update suceessful!");
            }
        }
        String v_account = " update dm.rpt_tm_call_list_new_prod t" +
                " inner join dm.dim_employee_distribute d" +
                " on t.caller = d.Employee_Name" +
                " set t.account_name = d.account_name" +
                " where t.account_name is null " +
                " and t.list_id in (" + str_list + ")";
        if(fc.dml_Sql(v_account,conn_db)){
                System.out.println("account_name update suceessful!");
            }
        try {
            conn_db.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Boolean randomFlag(List list_id) {
        String str_list = String.join(",",list_id);
        For_ConnMysql fc = new For_ConnMysql();
        Properties prop = getConfigInfo();
        String db_url = prop.getProperty("zx_jdbc_url");
        String db_user = prop.getProperty("zx_jdbc_username");
        String db_pass = prop.getProperty("zx_jdbc_password");
        Connection conn_db = fc.conn_Mysql(db_url,db_user,db_pass);
        String v_sql = "select sum(cnt) as cnt_tot,max(cnt) as cnt_max " +
                "  from (select old_caller, count(0) as cnt " +
                "  from dm.rpt_tm_call_list_new_prod" +
                "  where caller is null and list_id in (" + str_list +
                "  ) group by old_caller) x";
        ResultSet rs = fc.query_Sql(v_sql,conn_db);
        Integer cnt_tot = 0;
        Integer cnt_max = 0;
        try {
            while (rs.next()){
                cnt_tot = Integer.parseInt(rs.getString(1));
                cnt_max = Integer.parseInt(rs.getString(2));
            }
            if (cnt_tot == 0){
                return false;
            }
            if(cnt_max > (cnt_tot/2)){
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
        List<String> list_id=new ArrayList<String>();
        List<String> list_org=new ArrayList<String>();
        List<String> list_res=new ArrayList<String>();

        ForRandomList fr = new ForRandomList();
        //获取三个list
        fr.getListInit(list_id,list_org,list_res);
        int loop_cnt = 0;
        if (list_id.size()%DISTRIBUTE==0){
            loop_cnt = (int)Math.floor(list_id.size()/DISTRIBUTE);
        }else {
            loop_cnt = (int)Math.floor(list_id.size()/DISTRIBUTE) + 1;
        }

        System.out.println("名单总数为:" + list_id.size() + "循环次数为:" + loop_cnt);
        for (int i = 0; i < loop_cnt; i++) {
            if(i == loop_cnt - 1){
                List<String> list_id_tmp=new ArrayList<String>();
                List<String> list_org_tmp=new ArrayList<String>();
                List<String> list_res_tmp=new ArrayList<String>();
                for (int j = i*30; j < list_id.size(); j++) {
                    list_id_tmp.add(list_id.get(j));
                    list_org_tmp.add(list_org.get(j));
                    list_res_tmp.add(list_org.get(j));
                }
                if(fr.randomFlag(list_id_tmp)){
                    fr.getResList(list_org_tmp,list_res_tmp);
                    fr.updateListid(list_id_tmp,list_res_tmp);
                    System.out.println("第" + (i+1) + "次分发成功!" );
                }else {
                    fr.delListid(list_id_tmp);
                    System.out.println("第" + (i+1) + "次分发失败!" );
                }
            }else{
                List<String> list_id_tmp=new ArrayList<String>();
                List<String> list_org_tmp=new ArrayList<String>();
                List<String> list_res_tmp=new ArrayList<String>();
                for (int j = i*30; j < i*30 + DISTRIBUTE; j++) {
                    list_id_tmp.add(list_id.get(j));
                    list_org_tmp.add(list_org.get(j));
                    list_res_tmp.add(list_org.get(j));
                }
                if(fr.randomFlag(list_id_tmp)){
                    fr.getResList(list_org_tmp,list_res_tmp);
                    fr.updateListid(list_id_tmp,list_res_tmp);
                    System.out.println("第" + (i+1) + "次分发成功!" );
                }else {
                    fr.delListid(list_id_tmp);
                    System.out.println("第" + (i+1) + "次分发失败!" );
                }

            }
        }
    }
}