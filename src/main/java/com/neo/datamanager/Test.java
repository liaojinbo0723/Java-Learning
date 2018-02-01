package com.neo.datamanager;


public class Test  {

    public static void main(String[] args) {
        String strJobId = "123123";
        System.out.println("strJobId = " + strJobId);
        String strUpdateSql = "update mss.cm_run_job set state = 3 where job_id = '"
                            + strJobId
                            + "' and state = 2";
        System.out.println(strUpdateSql);
        System.out.println("strUpdateSql = " + strUpdateSql);
        System.out.println("123".concat("aa"));
        
        String startDate = args[0];
        System.out.println("startDate = " + startDate);
    }
}
