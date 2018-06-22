package com.neo.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * 注解一定要加上
 * deterministic false 表示这个函数运行结果不是确定的，否则函数只会执行一遍
 * stateful = true  让udf有状态，最简单的例子就是在每行计算他和他之前的累加和
 *    允许hive保留全局的静态变量
 */
@UDFType(deterministic = false, stateful = true)//stateful参数是必要的
public class UDFRowSeq extends UDF {
    private int result;

    public UDFRowSeq() {
        result = 0;
    }

    public int evaluate() {
        result++;
        return result;
    }

    public static void main(String[] args) {
        System.out.println(new UDFRowSeq().evaluate());
    }
}
