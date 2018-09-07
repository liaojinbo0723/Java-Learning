package com.neo.datamanager;

import java.security.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Test2 {
    public static void main(String[] args) {
        System.out.println(DateOper.strToLong("2018-08-01 00:00:00"));
        System.out.println(DateOper.dateToStr2(new Date()));
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
		      Date date = sdf1.parse("2018-01-22T09:12:43.083Z");//拿到Date对象
		      String str = sdf2.format(date);//输出格式：2017-01-22 09:28:33
		      System.out.println(str);
		  } catch (Exception e) {
		      e.printStackTrace();
		  }
    }
}
