package com.alibaba.streamcompute;

import com.alibaba.flink.ml.operator.coding.RowCSVCoding;
import com.alibaba.flink.ml.operator.util.DataTypes;
import com.alibaba.flink.ml.operator.util.TypeUtil;
import com.alibaba.flink.ml.tensorflow.client.TFConfig;
import com.alibaba.flink.ml.tensorflow.client.TFUtils;
import com.alibaba.flink.ml.util.MLConstants;
//import com.alibaba.streamcompute.i2i.Recall;
import com.alibaba.streamcompute.i2i.tikv.RecallFromTiKV;
import com.alibaba.streamcompute.tools.Constants;
import com.alibaba.streamcompute.tools.Util;
import com.alibaba.streamcompute.wdl.Predict;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class RecommendationLocal {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
    RowTypeInfo typeInfo = getTypeInfo();

    DataStream<String> kafkaSourceStream =
        streamEnv.addSource(
            new FlinkKafkaConsumer011<>(
                Constants.KAFKA_SOURCE_TOPIC, new SimpleStringSchema(), Util.getKafkaProPerties()));
    DataStream<Tuple2<String, List<String>>> itemsStream =
        kafkaSourceStream.map(new RecallFromTiKV()::recall);
    DataStream<Row> samples =
        itemsStream
            .map(tuple -> new Predict().generateSample(tuple.f0, tuple.f1))
            .returns(typeInfo);
    Table sampleTable = tableEnv.fromDataStream(samples);
    TFConfig tfConfig = createTFConfig(createSerializeConf());
    Table predictResult =
        TFUtils.inference(
            streamEnv, tableEnv, sampleTable, tfConfig, TypeUtil.rowTypeInfoToSchema(typeInfo));
    DataStream<Row> probability = tableEnv.toAppendStream(predictResult, typeInfo);
    DataStream<Tuple2<String, List<String>>> recommendation = probability.map(Predict::getTopK);
    recommendation.print();

    streamEnv.execute();
  }

  private static RowTypeInfo getTypeInfo() {
    TypeInformation[] types = new TypeInformation[1];
    types[0] = BasicTypeInfo.STRING_TYPE_INFO;
    String[] names = {"sample"};
    return new RowTypeInfo(types, names);
  }

  private static Map<String, String> createSerializeConf() {
    Map<String, String> prop = new HashMap<>();
    StringBuilder inputSb = new StringBuilder();
    inputSb.append(DataTypes.STRING.name());
    prop.put(MLConstants.ENCODING_CLASS, RowCSVCoding.class.getCanonicalName());
    prop.put(MLConstants.DECODING_CLASS, RowCSVCoding.class.getCanonicalName());
    prop.put(RowCSVCoding.ENCODE_TYPES, inputSb.toString());
    prop.put(RowCSVCoding.DECODE_TYPES, inputSb.toString());
    prop.put(RowCSVCoding.TYPES_SPLIT_CONFIG, "-");
    return prop;
  }

  private static TFConfig createTFConfig(Map<String, String> serializeConf) throws IOException {
    File directory = new File("");
    String courseFile = directory.getCanonicalPath();
    String script = courseFile + "/src/main/code/prediction.py";
    String ckptPathPrefix = courseFile + "/src/main/code";

    Map<String, String> prop = new HashMap<>();
    prop.putAll(serializeConf);
    prop.put("ckpt_path_prefix", ckptPathPrefix);
    prop.put("ckpt", Constants.MODEL_CKPT);
    return new TFConfig(1, 1, prop, script, "map_func", null);
  }
}
