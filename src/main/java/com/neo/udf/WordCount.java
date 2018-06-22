package com.neo.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.List;

public class WordCount extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1){
            throw new UDFArgumentLengthException("wordCountExample only takes 1 arguments");
        }
        ObjectInspector word = objectInspectors[0];
        if (!(word instanceof StringObjectInspector)){
            throw new UDFArgumentException("argument must be a string");
        }
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String[] list = deferredObjects[0].toString().split(",");
        return list;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "wordCountExample";
    }
}
