package com.neo.udf;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

@UDFType(deterministic = false, stateful = true)
public class RowSeq extends GenericUDF {

    private static LongWritable result = new LongWritable();
    private static final char SEPARATOR = '_';
    private static final String ATTEMPT = "attempt";
    private long initID = 0l;
    private int increment = 0;


    @Override
    public void configure(MapredContext context) {
        increment = context.getJobConf().getNumMapTasks();
        if (increment == 0) {
            throw new IllegalArgumentException("mapred.map.tasks is zero");
        }

        initID = getInitId(context.getJobConf().get("mapred.task.id"), increment);
        if (initID == 0l) {
            throw new IllegalArgumentException("mapred.task.id");
        }

        System.out.println("initID : " + initID + "  increment : " + increment);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        result.set(getValue());
        increment(increment);
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "RowSeq-func()";
    }

    private synchronized void increment(int incr) {
        initID += incr;
    }

    private synchronized long getValue() {
        return initID;
    }

    //attempt_1478926768563_0537_m_000004_0 // return 0+1
    private long getInitId(String taskAttemptIDstr, int numTasks)
            throws IllegalArgumentException {
        try {
            String[] parts = taskAttemptIDstr.split(Character.toString(SEPARATOR));
            if (parts.length == 6) {
                if (parts[0].equals(ATTEMPT)) {
                    if (!parts[3].equals("m") && !parts[3].equals("r")) {
                        throw new Exception();
                    }
                    long result = Long.parseLong(parts[4]);
                    if (result >= numTasks) { //if taskid >= numtasks
                        throw new Exception("TaskAttemptId string : " + taskAttemptIDstr
                                + "  parse ID [" + result + "] >= numTasks[" + numTasks + "] ..");
                    }
                    return result + 1;
                }
            }
        } catch (Exception e) {
        }
        throw new IllegalArgumentException("TaskAttemptId string : " + taskAttemptIDstr
                + " is not properly formed");
    }

}
