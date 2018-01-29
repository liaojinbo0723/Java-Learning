package com.neo.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import com.xiaoniu.encript.EncriptUtils;

public class DataDecryptQgz extends UDF{

    /**
     * 重写evaluate方法
     * @param strEncrypt
     * @return
     */
    private String evaluate(String strEncrypt){
        String strData = EncriptUtils.decript(strEncrypt);
        return strData;
    }

    public static void main(String[] args) {
        DataDecryptQgz ddq = new DataDecryptQgz();
        System.out.println(ddq.evaluate("TOqJ+Ks24/vY12j6PbW68HxCOFowpn711haHrG9d9fc="));
    }
}
