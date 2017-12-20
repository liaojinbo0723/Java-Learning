package com.neo.datamanager;


public class Test  {


    public static void main(String[] args) {
        String str_jobid = "123123";
        String v_update = "update mss.cm_run_job set state = 3 where job_id = '" + str_jobid
                    + "' and state = 2";
        System.out.println(v_update);
    }
}
