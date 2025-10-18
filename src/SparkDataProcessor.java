import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;
import java.util.Arrays;
import java.util.List;
import java.io.FileWriter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class SparkDataProcessor {

    public static void main(String[] args) {
        String outputPath = args.length > 0 ? args[0] : "/tmp/spark_output.json";

        // Create Spark session
        SparkSession spark = SparkSession.builder()
            .appName("DataProcessor")
            .getOrCreate();

        try {
            System.out.println("Generating sample data...");

            // Create sample data
            List<Row> data = Arrays.asList(
                RowFactory.create("product_A", "region_1", 100, 10.5),
                RowFactory.create("product_A", "region_2", 150, 10.5),
                RowFactory.create("product_A", "region_1", 200, 10.5),
                RowFactory.create("product_B", "region_1", 80, 25.0),
                RowFactory.create("product_B", "region_2", 120, 25.0),
                RowFactory.create("product_B", "region_1", 90, 25.0),
                RowFactory.create("product_C", "region_1", 300, 5.0),
                RowFactory.create("product_C", "region_2", 250, 5.0),
                RowFactory.create("product_C", "region_1", 280, 5.0)
            );

            // Define schema
            StructType schema = new StructType()
                .add("product", DataTypes.StringType)
                .add("region", DataTypes.StringType)
                .add("quantity", DataTypes.IntegerType)
                .add("price", DataTypes.DoubleType);

            Dataset<Row> df = spark.createDataFrame(data, schema);

            System.out.println("\nInput Data:");
            df.show();

            // Process data - aggregate by product
            System.out.println("Processing data with Spark...");

            Dataset<Row> results = df
                .groupBy("product")
                .agg(
                    sum(col("quantity").multiply(col("price"))).alias("total_revenue"),
                    sum("quantity").alias("total_quantity"),
                    count("*").alias("transaction_count"),
                    avg("quantity").alias("avg_quantity")
                )
                .orderBy(col("total_revenue").desc());

            System.out.println("\nProcessed Results:");
            results.show();

            // Save to JSON
            results.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .json(outputPath);

            System.out.println("\nResults saved to " + outputPath);
            System.out.println("Spark job completed successfully!");

        } finally {
            spark.stop();
        }
    }
}
