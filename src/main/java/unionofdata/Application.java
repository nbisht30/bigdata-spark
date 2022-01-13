package unionofdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Application {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 Datasets")
                .master("local")
                .getOrCreate();

        Dataset<Row> durhamDf = buildDurhamParksDataFrame(spark);
        durhamDf.printSchema();


        Dataset<Row> philDf = buildPhilParksDataFrame(spark);
        philDf.printSchema();

        System.out.println("Both data frames:-");
        durhamDf.show(10);
        philDf.show(10);

        unionDataFramesByColumnNames(durhamDf, philDf);
    }

    private static void unionDataFramesByColumnNames(Dataset<Row> df1, Dataset<Row> df2) {
        // Match by column names using the unionByName() method
        // union() method can be used if we want to union by order.

        Dataset<Row> resultDataFrame = df1.unionByName(df2);
        resultDataFrame.show(100);
        df1.printSchema();

        // Checking number of partitions
        Partition[] partitions = resultDataFrame.rdd().partitions();
        System.out.println("Total number of partitions: " + partitions.length);

        // Dividing into more partitions
        resultDataFrame = resultDataFrame.repartition(5);
        partitions = resultDataFrame.rdd().partitions();
        System.out.println("Total number of partitions after re-partitioning: " + partitions.length);
    }

    public static Dataset<Row> buildDurhamParksDataFrame(SparkSession sparkSession) {
        Dataset<Row> dataFrame = sparkSession.read().format("json").option("multiline", true)
                .load("src/main/resources/durham-parks.json");
        dataFrame = dataFrame.
                withColumn("park_id", concat(dataFrame.col("datasetid"), lit("_"), dataFrame.col("fields.objectid"), lit("_Durham"))).
                withColumn("park_name", dataFrame.col("fields.park_name")).
                withColumn("city", lit("Durham")).
                withColumn("address", dataFrame.col("fields.address")).
                withColumn("has_playground", dataFrame.col("fields.playground")).
                withColumn("zipcode", dataFrame.col("fields.zip")).
                withColumn("land_in_acres", dataFrame.col("fields.acres")).
                withColumn("geoX", dataFrame.col("geometry.coordinates").getItem(0)).
                withColumn("geoY", dataFrame.col("geometry.coordinates").getItem(1)).
                drop("fields").
                drop("geometry").
                drop("record_timestamp").
                drop("recordid").
                drop("datasetid") // dropping some old columns that we won't need now
        ;
        return dataFrame;
    }

    public static Dataset<Row> buildPhilParksDataFrame(SparkSession sparkSession) {
        Dataset<Row> dataFrame = sparkSession.read().format("csv")
                .option("multiline", true)
                .option("header", true)
                .load("src/main/resources/philadelphia_recreations.csv");
//        dataFrame = dataFrame.filter(lower(dataFrame.col("USE_")).like("%park%"));
//         Or use SQL like syntax
        dataFrame = dataFrame.filter("lower(USE_) like '%park%'");
        dataFrame = dataFrame.
                withColumn("park_id", concat(dataFrame.col("OBJECTID"), lit("_Philadelphia"))).
                withColumnRenamed("ASSET_NAME", "park_name").
                withColumn("city", lit("Philadelphia")).
                withColumnRenamed("ADDRESS", "address").
                withColumn("has_playground", lit("UNKNOWN")).
                withColumnRenamed("ZIPCODE", "zipcode").
                withColumnRenamed("ACREAGE", "land_in_acres").
                withColumn("geoX", lit("UNKNOWN")).
                withColumn("geoY", lit("UNKNOWN")).
                drop("OBJECTID").
                drop("SITE_NAME").
                drop("CHILD_OF").
                drop("USE_").
                drop("TYPE").
                drop("DESCRIPTION").
                drop("SQ_FEET").
                drop("ALLIAS").
                drop("CHRONOLOGY").
                drop("NOTES").
                drop("DATE_EDITED").
                drop("EDITED_BY").
                drop("OCCUPANT").
                drop("TENANT").
                drop("LABEL") // dropping some old columns that we won't need now
        ;
        return dataFrame;
    }
}
