import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class TestReadTextFile {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        StructType userSchema = new StructType().add("name", "string").add("age", "int").add("timestamp","timestamp");
        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ",")
                .schema(userSchema)      // Specify schema of the csv files
                .csv("C://tmp//");
        //每隔一分钟计算一分钟之前的 2分钟时间段内的数据
        Dataset<Row> windowedCounts = csvDF.groupBy(
                functions.window(csvDF.col("timestamp"), "2 minutes","1 minutes"),
                csvDF.col("name")
        ).count();
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
        //todo 数据源改成json格式的订单信息，测试通过后消费cmq
    }
}
