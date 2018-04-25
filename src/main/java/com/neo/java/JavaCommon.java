package com.neo.java;

import java.util.*;

public class JavaCommon {

    private void dateFun(){
        Date d1 = new Date();
        System.out.println(d1.toString());
        System.out.println(d1.getTime());
    }

    public static void main(String[] args) {
        //arraylist和hashmap
//        ArrayList<String> arrList = new ArrayList<String>();
//        arrList.add("1");
//        arrList.add("2");
//        arrList.add("c");
//        arrList.forEach(s -> System.out.println(s));
//        Map<Integer,String> map = new HashMap<Integer,String>();
//        map.put(1,"a");
//        map.put(2,"b");
//        map.entrySet().forEach(s -> System.out.println(s.getKey()+ ":" + s.getValue()));
        JavaCommon jc = new JavaCommon();
        jc.dateFun();
        System.out.println("job_stg_zx02_t_user_role.kjb".contains(".kjb"));
    }
}
