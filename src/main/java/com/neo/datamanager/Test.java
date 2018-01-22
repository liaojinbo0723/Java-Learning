package com.neo.datamanager;


public class Test  {

    public static void main(String[] args) {
        String strJobId = "123123";
        String strUpdateSql = "update mss.cm_run_job set state = 3 where job_id = '"
                            + strJobId
                            + "' and state = 2";
        System.out.println(strUpdateSql);
    }
}
