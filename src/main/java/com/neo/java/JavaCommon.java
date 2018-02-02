package com.neo.java;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaCommon {

    public static void main(String[] args) {
        //arraylist和hashmap
        ArrayList<String> arrList = new ArrayList<String>();
        arrList.add("1");
        arrList.add("2");
        arrList.add("c");
        arrList.forEach(s -> System.out.println(s));
        Map<Integer,String> map = new HashMap<Integer,String>();
        map.put(1,"a");
        map.put(2,"b");
        map.entrySet().forEach(s -> System.out.println(s.getKey()+ ":" + s.getValue()));
    }
}
