package com.neo.datamanager;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpSms {

    public static Logger logger = Logger.getLogger(HttpSms.class);

    public static void doPost(String url,Map<String,String> map) {
        HttpClient httpClient = null;
        HttpPost httpPost = null;
        String result = null;

        //get mothod
        httpPost = new HttpPost(url);
        // set header
        httpPost.addHeader("Content-Type", "application/x-www-form-urlencoded");
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("partnerId", map.get("partnerId")));
        params.add(new BasicNameValuePair("signature", map.get("signature")));
        params.add(new BasicNameValuePair("mobile", map.get("mobile")));
        params.add(new BasicNameValuePair("content", map.get("content")));
        params.add(new BasicNameValuePair("smsType", map.get("smsType")));
        try {
            UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params);
            httpPost.setEntity(entity);
            HttpResponse response = null;
            response = httpClient.execute(httpPost);
            HttpEntity httpEntity = response.getEntity();
            logger.info(EntityUtils.toString(httpEntity, "UTF-8"));

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String encrypt(String decript) {
        try {
            MessageDigest digest = java.security.MessageDigest.getInstance("SHA-1");
            digest.update(decript.getBytes("UTF-8"));
            byte messageDigest[] = digest.digest();
            StringBuffer hexString = new StringBuffer();
            for (int i = 0; i < messageDigest.length; i++) {
                String shaHex = Integer.toHexString(messageDigest[i] & 0xFF);
                if (shaHex.length() < 2) {
                    hexString.append(0);
                }
                hexString.append(shaHex);
            }
            return hexString.toString();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void main(String[] args) {
        Map<String,String> map = new HashMap<String,String>();
        map.put("partnerId","");
        map.put("mobile","18688286417");
        map.put("content","");
        map.put("smsType","sms");
        map.put("signature","");
        logger.info(HttpSms.encrypt("count=6&mobile=13813801380&moduleId=中文&partnerId=123secretKey"));
    }
}
