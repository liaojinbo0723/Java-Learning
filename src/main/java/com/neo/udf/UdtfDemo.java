package com.neo.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class UdtfDemo extends GenericUDTF {

    /**
     * 返回 UDTF 的返回行的信息（字段名，字段类型）
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentLengthException("ExplodeMap takes only two argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }

        // 保存字段名
        ArrayList<String> fieldNames = new ArrayList<String>();

        // 保存字段家
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        // 增加字段名为 col1 的字段
        fieldNames.add("col1"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        // 以下是重复的套路！！！
        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // 最后返回 (字段名, 字段类型)
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    /**
     * process 方法, 真正的处理过程在 process 函数中
     * 在 process 中每一次 forward() 调用产生一行；如果产生多列可以将多个列的值放在一个数组中，然后将该数组传入到 forward() 函数
     */
    @Override
    public void process(Object[] args) throws HiveException {
        String[] keys = args[0].toString().split(",");
        String[] values = args[1].toString().split(",");
        if(keys.length!=values.length){
            throw new UDFArgumentException("ArrayExplodeUDTF() two argument after split's length must to same");
        }
        for (int i = 0; i < keys.length; i++) {
            try {
                String[] result = new String[]{keys[i], values[i]};
                System.out.println(result);
                // 生成一行
                forward(result);
            }catch (Exception e){
                continue;
            }


        }
    }


    /**
     * 处理完成后的关闭流程
     */
    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) throws Exception {

        // 获取传入的值, key:value;key:value;
        Object[] argdss = {"1,2,3","a,b,c"};

        UdtfDemo obj = new UdtfDemo();
        obj.process(argdss);

    }

}
