package cn.swiftpass.spark.streaming;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class MonitorOrderSuccessRate {
    private static final String kafkaIp = "127.0.0.1:9092";
    private static final String kafkaWriteTopic = "monitorOrder";
    private static final String mysqlDriver = "com.mysql.jdbc.Driver";
    private static final String mysqlUrl = "jdbc:mysql://127.0.0.1:3306/monitor?useUnicode=true&characterEncoding=utf8";
    private static final String userName = "root";
    private static final String password = "12345678";


    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("monitorOrderSuccessRate")
                .getOrCreate();

        //配置kafka作为source
        //http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaIp)
                .option("subscribe", "test")
//                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();
        StructType orderSchema = new StructType()
                .add("id", "string")
                .add("money", "long")
                .add("state", "string")
                .add("bank_no", "string")
                .add("agent_id", "string")
                .add("merchant_id", "string")
                .add("group_id", "string")
                .add("term_type", "string")
                .add("trade_time", "timestamp");

        //使用schema解析kafka数据
        Dataset<Row> orderDF = df
                .select(from_json(col("value").cast("string"), orderSchema).alias("parsed_value"))
                .select("parsed_value.*");

//        groupWithBankNo(orderDF);
        groupWithMerchantId(orderDF, spark);

        spark.streams().awaitAnyTermination();

    }

    /*
     * 单个流中，设置时间窗口，根据bank_no分组，求count值
     * */
    private static void groupWithBankNo(Dataset<Row> orderDF) throws TimeoutException {
        Dataset<Row> countDf = orderDF.
                withWatermark("trade_time", "30 seconds").
                groupBy(window(col("trade_time"), "1 minutes"), col("bank_no").alias("groupBy"), col("term_type"), col("state")
                ).count().withColumn("groupByType", lit("bank_no"));
//        write2kafka(countDf);
        write2console(countDf);
    }

    /*
     *stream-static Join
     * kafka和mysql表join
     * mysql中的表如果修改了，在流中对应的dataset值也会相应改变。
     *  */
    private static void groupWithMerchantId(Dataset<Row> orderDF, SparkSession ss) throws TimeoutException {
        Dataset<Row> filterDf = ss
                .read()
                .format("jdbc")
                .option("driver", mysqlDriver)
                .option("url", mysqlUrl)
                .option("dbtable", "filter_merchant_ids")
                .option("user", userName)
                .option("password", password)
                .load()
                .filter("org_type='1'")
                .select("org_type", "merchant_id");


        Column joinCond = filterDf.col("merchant_id").equalTo(orderDF.col("merchant_id"));

        //这里join之后，要删除掉一个merchant_id，不然会有歧义
        Dataset<Row> afterFilterDf = orderDF.join(filterDf, joinCond).drop(filterDf.col("merchant_id"));
        Dataset<Row> countDf = afterFilterDf.
                withWatermark("trade_time", "30 seconds").
                groupBy(window(col("trade_time"), "1 minutes"), col("merchant_id").alias("groupBy"), col("term_type"), col("state")
                ).count().withColumn("groupByType", lit("merchant_id"));
// Join between two streaming DataFrames/Datasets is not supported in Update output mode, only in Append output mode
// Default trigger (runs micro-batch as soon as it can)
//                .trigger(Trigger.ProcessingTime("1 minutes"))

//        write2kafka(countDf);
        write2console(countDf);
    }

//todo 双流join


    private static void write2kafka(Dataset<Row> countDf) throws TimeoutException {
        countDf
                .toJSON().as("value")
                .select(col("value").cast("string"))
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaIp)
                .option("topic", kafkaWriteTopic)
//                .trigger(Trigger.ProcessingTime("1 minutes"))
                .outputMode("append")
                .option("checkpointLocation", "C://tmp//dir//")
                .start();
    }

    private static void write2console(Dataset<Row> countDf) throws TimeoutException {
        countDf.writeStream().outputMode("append").format("console").start();
    }
}

