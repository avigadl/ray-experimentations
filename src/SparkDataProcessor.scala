import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDataProcessor {
  def main(args: Array[String]): Unit = {
    val outputPath = if (args.length > 0) args(0) else "/tmp/spark_output.json"

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("DataProcessor")
      .getOrCreate()

    import spark.implicits._

    try {
      println("Generating sample data...")

      // Create sample data
      val data = Seq(
        ("product_A", "region_1", 100, 10.5),
        ("product_A", "region_2", 150, 10.5),
        ("product_A", "region_1", 200, 10.5),
        ("product_B", "region_1", 80, 25.0),
        ("product_B", "region_2", 120, 25.0),
        ("product_B", "region_1", 90, 25.0),
        ("product_C", "region_1", 300, 5.0),
        ("product_C", "region_2", 250, 5.0),
        ("product_C", "region_1", 280, 5.0)
      ).toDF("product", "region", "quantity", "price")

      println("\nInput Data:")
      data.show()

      // Process data - aggregate by product
      println("Processing data with Spark...")

      val results = data
        .groupBy("product")
        .agg(
          sum($"quantity" * $"price").alias("total_revenue"),
          sum("quantity").alias("total_quantity"),
          count("*").alias("transaction_count"),
          avg("quantity").alias("avg_quantity")
        )
        .orderBy($"total_revenue".desc)

      println("\nProcessed Results:")
      results.show()

      // Save to JSON
      results.coalesce(1)
        .write
        .mode("overwrite")
        .json(outputPath)

      println(s"\nResults saved to $outputPath")
      println("Spark job completed successfully!")

    } finally {
      spark.stop()
    }
  }
}
