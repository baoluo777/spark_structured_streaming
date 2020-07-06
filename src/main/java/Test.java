import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class Test {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        StructType userSchema = new StructType().add("name", "string").add("age", "integer").add("timestamp","timestamp");
        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ",")
                .schema(userSchema)      // Specify schema of the csv files
                .csv("C://tmp//");
        Dataset<Row> windowedCounts = csvDF.groupBy(
                functions.window(csvDF.col("timestamp"), "1 minutes"),
                csvDF.col("name")
        ).count();
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
