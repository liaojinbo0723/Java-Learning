package com.neo.java;

import java.util.regex.Pattern;

public class TestMatch {
    public static void main(String[] args) {
        String strTag = "2018-05-05";
        //String isMatch = "(ods|dwd|dmb)\\w+";
        String isMatch = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
        System.out.println(Pattern.matches(isMatch,strTag));
    }
}
