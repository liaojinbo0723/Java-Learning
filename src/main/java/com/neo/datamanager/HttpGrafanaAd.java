package com.neo.datamanager;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 利用HttpClient进行post请求的工具类
 */
public class HttpGrafanaAd {

    public static final String URL = "https://monitor.niudingfeng.com/api/datasources/proxy/79/_msearch";
    public static final String TOKEN = "eyJrIjoiZDVnMzQ1RTZabzBmRGl1TEhGWU5TRnI2MDJydmp5R3UiLCJuIjoiYWRsb2dzIiwiaWQiOjExfQ==";
    public static final String CHARSET = "utf-8";
    public static final String FILE = "result.txt";


    @SuppressWarnings("resource")
    public static String doPostSucc(String url, String jsonstr, String token, String charset) {
        HttpClient httpClient = null;
        HttpPost httpPost = null;
        String result = null;
        try {
            httpClient = new SSLClient();
            httpPost = new HttpPost(url);
            httpPost.addHeader("Content-Type", "application/json");
            String tokenStr = "Bearer " + token;
            httpPost.addHeader("Authorization", tokenStr);
            StringEntity se = new StringEntity(jsonstr);
            se.setContentType("application/json");
            se.setContentEncoding("utf-8");
            httpPost.setEntity(se);
            HttpResponse response = httpClient.execute(httpPost);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                    writeFile("succ.txt",result);
                    loginSucc(result);

                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    @SuppressWarnings("resource")
    public static String doPostFail(String url, String jsonstr, String token, String charset) {
        HttpClient httpClient = null;
        HttpPost httpPost = null;
        String result = null;
        try {
            httpClient = new SSLClient();
            httpPost = new HttpPost(url);
            httpPost.addHeader("Content-Type", "application/json");
            String tokenStr = "Bearer " + token;
            httpPost.addHeader("Authorization", tokenStr);
            StringEntity se = new StringEntity(jsonstr);
            se.setContentType("application/json");
            se.setContentEncoding("utf-8");
            httpPost.setEntity(se);
            HttpResponse response = httpClient.execute(httpPost);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                    //System.out.println(result);
                    writeFile("fail.txt",result);
                    loginFail(result);

                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }


    /**
     * 员工登录工作站成功的日志
     * @param result
     */
    public static void loginSucc(String result) {
        //解析json
        JsonParser parser = new JsonParser();  //创建JSON解析器
        JsonObject object = (JsonObject) parser.parse(result);  //创建JsonObject对象
        JsonArray array = object.get("responses").getAsJsonArray().get(0).getAsJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray();
        for (int i = 0; i < array.size(); i++) {
            JsonObject subObject = array.get(i).getAsJsonObject().get("_source").getAsJsonObject();
            LoginStationSucc lss = new LoginStationSucc();
            lss.setTimestamp(utcToString(subObject.get("@timestamp").getAsString()));
            lss.setComputerName(subObject.get("computer_name").getAsString());
            lss.setUserName(subObject.get("event_data").getAsJsonObject().get("TargetUserName").getAsString());
            lss.setIpAddress(subObject.get("event_data").getAsJsonObject().get("IpAddress").getAsString());
            writeFile(FILE,lss.toString());
        }

    }

    /**
     * 员工登录工作站失败的日志
     * @param result
     */
    public static void loginFail(String result) {
        //解析json
        JsonParser parser = new JsonParser();  //创建JSON解析器
        JsonObject object = (JsonObject) parser.parse(result);  //创建JsonObject对象
        JsonArray array = object.get("responses").getAsJsonArray().get(0).getAsJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray();
        for (int i = 0; i < array.size(); i++) {
            JsonObject subObject = array.get(i).getAsJsonObject().get("_source").getAsJsonObject();
            LoginStationFail lsf = new LoginStationFail();
            lsf.setTimestamp(utcToString(subObject.get("@timestamp").getAsString()));
            lsf.setComputerName(subObject.get("event_data").getAsJsonObject().get("WorkstationName").getAsString());
            lsf.setUserName(subObject.get("event_data").getAsJsonObject().get("TargetUserName").getAsString());
            lsf.setIpAddress(subObject.get("event_data").getAsJsonObject().get("IpAddress").getAsString());
            writeFile(FILE,lsf.toString());
        }

    }

    /**
     * UTC日期转换
     * @param dateUtc
     * @return
     */
    public static String utcToString(String dateUtc) {
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = sdf1.parse(dateUtc);//拿到Date对象
            String result = sdf2.format(date);//输出格式：2017-01-22 09:28:33
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 写入结果文件
     * @param file
     * @param res
     */
    public static void writeFile(String file, String res) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
            bw.write(res + "\n");
            bw.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取两个日期字符串之间的日期集合
     *
     * @param startTime:String
     * @param endTime:String
     * @return list:yyyy-MM-dd
     */
    public static List<String> getBetweenDate(String startTime, String endTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        // 声明保存日期集合
        List<String> list = new ArrayList<String>();
        try {
            // 转化成日期类型
            Date startDate = sdf.parse(startTime);
            Date endDate = sdf.parse(endTime);

            //用Calendar 进行日期比较判断
            Calendar calendar = Calendar.getInstance();
            while (startDate.getTime() <= endDate.getTime()) {
                // 把日期添加到集合
                list.add(sdf.format(startDate));
                // 设置日期
                calendar.setTime(startDate);
                //把日期增加一天
                calendar.add(Calendar.DATE, 1);
                // 获取增加后的日期
                startDate = calendar.getTime();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } 
        return list;
    }

    /**
     * 获取两个日期字符串之间的日期集合
     *
     * @param startTime:String
     * @param endTime:String
     * @return list:yyyy-MM-dd
     */
    public static List<String> getBetweenDateLong(String startTime, String endTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        // 声明保存日期集合
        List<String> list = new ArrayList<String>();
        try {
            // 转化成日期类型
            Date startDate = sdf.parse(startTime);
            Date endDate = sdf.parse(endTime);

            //用Calendar 进行日期比较判断
            Calendar calendar = Calendar.getInstance();
            while (startDate.getTime() <= endDate.getTime()) {
                // 把日期添加到集合
                list.add(DateOper.strToLong(sdf.format(startDate)).toString());
                // 设置日期
                calendar.setTime(startDate);
                //把日期增加一天
                calendar.add(Calendar.DATE, 1);
                // 获取增加后的日期
                startDate = calendar.getTime();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 导出数据增量
     */
    public static void exportData(String initStart,String initEnd) {
        File file  = new File(FILE);
        if(file.exists()){
            file.delete();
        }
        List<String> list = getBetweenDate(initStart, initEnd);
        list.forEach(time -> {
                    String indexName = "ndf.jcyw.ad-" + time.replace("-", ".");
                    String failStr = "{\"search_type\":\"query_then_fetch\",\"ignore_unavailable\":true,\"index\":[\"" + indexName + "\"],\"max_concurrent_shard_requests\":256}\n" +
                            "{\"size\":10000,\"query\":{\"bool\":{\"filter\":[{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"event_id: \\\"4625\\\"\"}}]}},\"sort\":{\"@timestamp\":{\"order\":\"asc\",\"unmapped_type\":\"boolean\"}},\"script_fields\":{},\"docvalue_fields\":[\"@timestamp\"]}\n";
                    String succStr = "{\"search_type\":\"query_then_fetch\",\"ignore_unavailable\":true,\"max_concurrent_shard_requests\":256,\"index\":[\"" + indexName + "\"]}\n" +
                            "{\"size\":10000,\"query\":{\"bool\":{\"filter\":[{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"event_id: \\\"4624\\\"  AND event_data.TargetUserName: xn AND message: \\\"Kerberos\\\"\"}}]}},\"sort\":{\"@timestamp\":{\"order\":\"asc\",\"unmapped_type\":\"boolean\"}},\"script_fields\":{},\"docvalue_fields\":[\"@timestamp\"]}\n";
                    doPostFail(URL, failStr, TOKEN, CHARSET);
                    doPostSucc(URL, succStr, TOKEN, CHARSET);
                    System.out.println(time + "导出完成!!!");

                }
        );
    }

    /**
     * 导出数据增量
     */
    public static void exportDataInit(String initStart,String initEnd) {
        File file  = new File(FILE);
        if(file.exists()){
            file.delete();
        }
        //0803之前索引为 ndf.jcyw.ad-2018
        List<String> listLong = getBetweenDateLong("2018-01-01","2018-01-02");
        String indexInit = "ndf.jcyw.ad-2018";
        for (int i = 1; i < listLong.size(); i++) {
//            String succInit = "{\"search_type\":\"query_then_fetch\",\"ignore_unavailable\":true,\"index\":[\"ndf.jcyw.ad-2018\"],\"max_concurrent_shard_requests\":256}\n" +
//                    "{\"size\":10000,\"query\":{\"bool\":{\"filter\":[{\"range\":{\"@timestamp\":{\"gte\":\"" + listLong.get(i-1) + "\",\"lte\":\"" + listLong.get(i) + "\",\"format\":\"epoch_millis\"}}},{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"event_id: \\\"4624\\\"  AND event_data.TargetUserName: xn AND message: \\\"Kerberos\\\"\"}}]}},\"sort\":{\"@timestamp\":{\"order\":\"desc\",\"unmapped_type\":\"boolean\"}},\"script_fields\":{},\"docvalue_fields\":[\"@timestamp\"]}\n";
//            String failInit = "{\"search_type\":\"query_then_fetch\",\"ignore_unavailable\":true,\"index\":[\"ndf.jcyw.ad-2018\"],\"max_concurrent_shard_requests\":256}\n" +
//                    "{\"size\":10000,\"query\":{\"bool\":{\"filter\":[{\"range\":{\"@timestamp\":{\"gte\":\"" + listLong.get(i-1)+ "\",\"lte\":\"" + listLong.get(i) + "\",\"format\":\"epoch_millis\"}}},{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"event_id: \\\"4625\\\"\"}}]}},\"sort\":{\"@timestamp\":{\"order\":\"desc\",\"unmapped_type\":\"boolean\"}},\"script_fields\":{},\"docvalue_fields\":[\"@timestamp\"]}\n";
            String failInit = "{\"search_type\":\"query_then_fetch\",\"ignore_unavailable\":true,\"index\":[\"ndf.jcyw.ad-2018\"],\"max_concurrent_shard_requests\":256}\n" +
                    "{\"size\":500,\"query\":{\"bool\":{\"filter\":[{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"event_id: \\\"4625\\\"\"}}]}},\"sort\":{\"@timestamp\":{\"order\":\"asc\",\"unmapped_type\":\"boolean\"}},\"script_fields\":{},\"docvalue_fields\":[\"@timestamp\"]}\n";

            doPostFail(URL, failInit, TOKEN, CHARSET);
            //doPostSucc(URL, succInit, TOKEN, CHARSET);
            System.out.println(new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.parseLong(listLong.get(i-1))))  + "导出完成!!!");
        }
        //0803之后索引为按天
//        List<String> list = getBetweenDate(initStart, initEnd);
//        list.forEach(time -> {
//                    String indexName = "ndf.jcyw.ad-" + time.replace("-", ".");
//                    String failStr = "{\"search_type\":\"query_then_fetch\",\"ignore_unavailable\":true,\"index\":[\"" + indexName + "\"],\"max_concurrent_shard_requests\":256}\n" +
//                            "{\"size\":10000,\"query\":{\"bool\":{\"filter\":[{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"event_id: \\\"4625\\\"\"}}]}},\"sort\":{\"@timestamp\":{\"order\":\"desc\",\"unmapped_type\":\"boolean\"}},\"script_fields\":{},\"docvalue_fields\":[\"@timestamp\"]}\n";
//                    String succStr = "{\"search_type\":\"query_then_fetch\",\"ignore_unavailable\":true,\"max_concurrent_shard_requests\":256,\"index\":[\"" + indexName + "\"]}\n" +
//                            "{\"size\":10000,\"query\":{\"bool\":{\"filter\":[{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"event_id: \\\"4624\\\"  AND event_data.TargetUserName: xn AND message: \\\"Kerberos\\\"\"}}]}},\"sort\":{\"@timestamp\":{\"order\":\"desc\",\"unmapped_type\":\"boolean\"}},\"script_fields\":{},\"docvalue_fields\":[\"@timestamp\"]}\n";
//                    doPostFail(URL, failStr, TOKEN, CHARSET);
//                    doPostSucc(URL, succStr, TOKEN, CHARSET);
//                    System.out.println(time + "导出完成!!!");
//
//                }
//        );
    }

    public static void main(String[] args) {
        if(args.length != 3){
            System.out.println("输入的参数个数有误,请输入三个参数,startDate,endDate,flag");
            System.exit(0);
        }
        String startDate = args[0];
        String endDate = args[1];
        String flag = args[2];
        if (flag.equals("0")){
            exportDataInit("2018-08-03","2018-09-06");
        }
//        exportData(startDate,endDate);
    }
}
