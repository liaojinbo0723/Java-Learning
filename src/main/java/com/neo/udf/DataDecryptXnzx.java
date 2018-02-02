package com.neo.udf;

import com.neo.xnol.security.service.DecryptService;
import org.apache.hadoop.hive.ql.exec.UDF;

public class DataDecryptXnzx extends UDF{

    private String  evaluate(String strEncry){
        String strDecry = DecryptService.decryptCardNo(strEncry);
        return strDecry;
    }

    public static void main(String[] args) {
        String idCardNo = "CzF1w0h6w9Xkv/F7JvMDklmlOoLmZbbYwxbp2Vfx/r+g=";
        String mobile = "G/a2ykSt0DZG86DdYB9sZWg==";
        String name = "G/a2yRoT93wYZ1wBnOe0cXw==";
        String email = null;
        System.out.println("email = " + DecryptService.decryptData(email));
        System.out.println("name = " + DecryptService.decryptData(name));
        System.out.println("mobile = " + DecryptService.decryptData(mobile));
        System.out.println("idCardNo = " + DecryptService.decryptIdNo(idCardNo));
    }
}
