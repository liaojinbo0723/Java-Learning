package com.neo.udf;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import java.util.Arrays;
import java.util.List;

/**
 * udaf 传入两个参数，第一个为子系统标识，第二个为属性值
 * 返回第一个属性值不为空且子系统标识最小的那个属性值
 */
public class UDAFMerge extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(UDAFMerge.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 2) {
            throw new UDFArgumentTypeException(info.length, "the number of params must is two!");
        }
//        System.out.println(info[0].getTypeName());
//        System.out.println(info[1].getTypeName());
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info[0]);
        if (!ObjectInspectorUtils.compareSupported(oi)) {
            throw new UDFArgumentTypeException(0, "Cannot support comparison of map<> type or complex type containing map<>.");
        }
        ObjectInspector oi1 = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info[1]);
        if (!ObjectInspectorUtils.compareSupported(oi1)) {
            throw new UDFArgumentTypeException(info.length - 1,
                    "Cannot support comparison of map<> type or complex type containing map<>.");
        }
        return new DataMergeEvaluator();
    }

    public static class DataMergeEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOI;
        private PrimitiveObjectInspector inputOI2;
        private PrimitiveObjectInspector outputOI;
        private String result;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            LOG.info(" Mode:" + m.toString() + " result has init");
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                inputOI2 = (PrimitiveObjectInspector) parameters[1];
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            }
            if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                outputOI = (PrimitiveObjectInspector) parameters[0];
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            } else {
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            }
        }

        /**
         * 存储每一次遍历的结果类
         */
        static class MergeAgg implements AggregationBuffer {
            int dataSrc = 9999;
            String mergeRes = "";
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MergeAgg magg = new MergeAgg();
            reset(magg);
            return magg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MergeAgg merge = (MergeAgg) agg;
            merge.dataSrc = 9999;
            merge.mergeRes = "";
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null) {
                return;
            }
            merge(agg, parameters);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            System.out.println("partial----->" + partial);
            if (partial != null) {
                MergeAgg agg2 = (MergeAgg) agg;
                List<Object> objects;
                if (partial instanceof Object[]) {

                    objects = Arrays.asList((Object[]) partial);
                    System.out.println("size----->" + objects.size());

                    int flag = PrimitiveObjectInspectorUtils.getInt(objects.get(0),inputOI);
                    String res = PrimitiveObjectInspectorUtils.getString(objects.get(1),inputOI2);
                    System.out.println("flag----->" + flag);
                    System.out.println("res------>" + res);
                    if (res != null && !res.equals("")) {
                        if (flag <= agg2.dataSrc) {
                            agg2.dataSrc = flag;
                            agg2.mergeRes = res;
                        }
                    }
                    System.out.println("result:" + agg2.mergeRes);
                }else{
                    String res = PrimitiveObjectInspectorUtils.getString(partial,outputOI);
                    agg2.mergeRes = res;
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MergeAgg merg = (MergeAgg) agg;
            result = merg.mergeRes;
            System.out.println("terminate----->result--->" + result);
            return result;
        }
    }

    public static void main(String[] args) throws SemanticException {
        System.out.println("123");
    }
}

