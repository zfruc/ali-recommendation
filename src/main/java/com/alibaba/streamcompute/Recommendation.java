package com.alibaba.streamcompute;

import com.alibaba.flink.ml.operator.coding.RowCSVCoding;
import com.alibaba.flink.ml.operator.util.DataTypes;
import com.alibaba.flink.ml.operator.util.TypeUtil;
import com.alibaba.flink.ml.tensorflow.client.TFConfig;
import com.alibaba.flink.ml.tensorflow.client.TFUtils;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.streamcompute.i2i.tikv.RecallFromTiKV;
import com.alibaba.streamcompute.tools.Constants;
import com.alibaba.streamcompute.tools.Util;
import com.alibaba.streamcompute.wdl.Predict;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang.StringUtils;
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

// import com.alibaba.streamcompute.i2i.Recall;

public class Recommendation {

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
    TFConfig tfConfig = createTFConfig(args, createSerializeConf());
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

  private static TFConfig createTFConfig(String[] args, Map<String, String> serializeConf) {

    Namespace res = parseArg(args);
    String trainPy = res.getString("TRAIN_PY");
    String envPath = res.getString("ENVPATH");
    String zkConnStr = res.getString("ZK_CONN_STR");
    String codePath = res.getString("codePath");

    Map<String, String> prop = new HashMap<>();
    if (StringUtils.isNotBlank(zkConnStr) && StringUtils.isNotBlank(codePath)) {
      prop.put(MLConstants.CONFIG_STORAGE_TYPE, MLConstants.STORAGE_ZOOKEEPER);
      prop.put(MLConstants.CONFIG_ZOOKEEPER_CONNECT_STR, zkConnStr);
      prop.put(MLConstants.USE_DISTRIBUTE_CACHE, "false");
      prop.put(MLConstants.REMOTE_CODE_ZIP_FILE, codePath);
      prop.put("ckpt", Constants.MODEL_CKPT);
    } else {
      throw new IllegalArgumentException("please check your args zk and code path");
    }
    prop.putAll(serializeConf);
    return new TFConfig(3, 1, prop, trainPy, "map_func", envPath);
  }

  private static Namespace parseArg(String[] args) {
    ArgumentParser parser = ArgumentParsers.newFor("add").build();
    parser.addArgument("--zk-conn-str").metavar("ZK_CONN_STR").dest("ZK_CONN_STR");
    parser
        .addArgument("--setup")
        .metavar("MNIST_SETUP_PYTHON")
        .dest("SETUP_PY")
        .help("The python script to setup Mnist data set on HDFS ready for training.");
    parser
        .addArgument("--train")
        .metavar("MNIST_TRAIN_PYTHON")
        .dest("TRAIN_PY")
        .help("The python script to run TF train.");
    parser
        .addArgument("--envpath")
        .metavar("ENVPATH")
        .dest("ENVPATH")
        .help("The HDFS path to the virtual env zip file.");
    parser
        .addArgument("--code-path")
        .metavar("codePath")
        .dest("codePath")
        .help("Python scriptRunner implementation class name");
    Namespace res = null;
    try {
      res = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }
    return res;
  }
}
