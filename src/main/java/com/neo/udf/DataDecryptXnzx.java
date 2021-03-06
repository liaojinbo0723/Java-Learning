package com.neo.udf;

import com.neo.xnol.security.service.DecryptService;
import org.apache.hadoop.hive.ql.exec.UDF;

public class DataDecryptXnzx extends UDF{

    private String  evaluate(String strEncry){
        String strDecry = DecryptService.decryptCardNo(strEncry);
        return strDecry;
    }

    public static void main(String[] args) {
        String idCardNo = "CzF1w0x2x8Xst/F7JuMDql2lMqb2TbbYwxbp2Vfx/r+g=";
//        String mobile = "T56dIOajlCm8tji3s+p61HQ==";
//        String name = "G/a2yRoXQ3BcN1wBnOe0cXw==";
//        String email = null;
//        System.out.println("email = " + DecryptService.decryptData(email));
//        System.out.println("name = " + DecryptService.decryptData(name));
//        System.out.println("mobile = " + DecryptService.decryptData(mobile));
//        System.out.println("idCardNo = " + DecryptService.decryptIdNo(idCardNo));
        System.out.println(DecryptService.decryptIdNo(idCardNo));
    }
}
