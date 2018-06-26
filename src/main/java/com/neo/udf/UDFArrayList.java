package com.neo.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import java.util.ArrayList;

public class UDFArrayList extends GenericUDF {

    private  ArrayList res = new ArrayList();

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        /**
         * 校验参数个数
         */
        if (objectInspectors.length != 1){
            throw new UDFArgumentLengthException("wordCountExample only takes 1 arguments");
        }
        /**
         * 校验参数类型
         */
        ObjectInspector word = objectInspectors[0];
        if (!(word instanceof StringObjectInspector)){
            throw new UDFArgumentException("argument must be a string");
        }
        /**
         * 定义udf返回的类型
         */
        ObjectInspector returnOI = PrimitiveObjectInspectorFactory
		          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
		return ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        res.clear();
        if (args.length < 1){
            return res;
        }
        String str = args[0].get().toString();
        String[] s = str.split(",");
        for (String word:s){
//            System.out.println(word);
            res.add(word);
        }
        return res;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "wordCountExample";
    }

    public static void main(String[] args) {
//        String strTest = "a,b,c,dsdfsdf";
//        try {
//            System.out.println(new UDFArrayList().evaluate(
//                    new DeferredObject[]{
//                            new DeferredJavaObject(strTest)
//                    })
//            );
//        } catch (HiveException e) {
//            e.printStackTrace();
//        }
    }
}
