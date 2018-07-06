package com.neo.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Iterator;

public class ArrayExplodeUDTF extends GenericUDTF {

    private PrimitiveObjectInspector stringOI = null;
//
//    @Override
//    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
//        if (argOIs.getAllStructFieldRefs().size() != 2) {
//            throw new UDFArgumentException("ArrayExplodeUDTF() takes exactly two argument");
//        }
//        ArrayList<String> fieldname = new ArrayList<String>();
//        fieldname.add("key");
//        fieldname.add("value");
//        ArrayList<ObjectInspector> fieldoi = new ArrayList<ObjectInspector>();
//        fieldoi.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//        fieldoi.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldname, fieldoi);
//    }


    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2){
            throw new UDFArgumentException("ArrayExplodeUDTF() takes exactly two argument");
        }
        ArrayList<String> fieldname = new ArrayList<String>();
        fieldname.add("key");
        fieldname.add("value");
        ArrayList<ObjectInspector> fieldoi = new ArrayList<ObjectInspector>();
        fieldoi.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldoi.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldname, fieldoi);
    }

     @Override
    public void process(Object[] objects) throws HiveException {
        if (objects.length == 2) {
            String[] key = objects[0].toString().split(",");
            String[] value = objects[1].toString().split(",");
            if (key.length != value.length) {
                throw new UDFArgumentException("ArrayExplodeUDTF() two argument after split's length must to same");
            }
            for (int i = 0; i < key.length; i++) {
                try {
                    String[] result = new String[2];
                    result[0] = key[i];
                    result[1] = value[i];
                    System.out.println(key[i] + "----" + value[i]);
                    //forward(result);
                    forward(result);
                }catch (Exception e){
                    continue;
                }


            }
        }

    }

//    @Override
//    public void process(Object[] objects) throws HiveException {
//        if (objects.length == 2) {
//            String key = stringOI.getPrimitiveJavaObject(objects[0]).toString();
//            String value = stringOI.getPrimitiveJavaObject(objects[1]).toString();
//            if (key.split(",").length != value.split(",").length) {
//                throw new UDFArgumentException("ArrayExplodeUDTF() two argument after split's length must to same");
//            }
//            ArrayList<Object[]> results = processInputRecord(key, value);
//            Iterator<Object[]> it = results.iterator();
//            while (it.hasNext()) {
//                Object[] r = it.next();
//                forward(r);
//            }
//        }
//
//    }

    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) throws HiveException {
        Object[] obj = {"1,2,3","a,b,c"};
        ArrayExplodeUDTF aeu = new ArrayExplodeUDTF();
        aeu.process(obj);
    }
}
