package basics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class FirstSparkApp {

    public static void main(String[] args) {
        // 1. Create a session, connects with Spark master node
        SparkSession sparkSession = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        // 2. Get data into a dataFrame (like a relational DB table)
        Dataset<Row> dataFrame = sparkSession.read().format("csv") // xml, json etc.
                .option("header", true) // file headers exist or not
                .load("src/main/resources/name_and_comments.txt"); // mention the file system here eg. HDFS, S3, in case of a cluster

        // 3. Show the first three rows
        dataFrame.show(3);

        // 4. Transform the data: Create a new column by concatenation of some existing columns
        Dataset<Row> transformedDF = dataFrame.withColumn("full_name", concat(dataFrame.col("last_name"), lit(", "),
                dataFrame.col("first_name")));
        // NOTE: This doesn't change dataFrame object as Datasets are immutable

        // Another transformation: Get comments with numbers in them
        transformedDF = transformedDF.filter(transformedDF.col("comment").rlike("\\d+")); //  rlike() - for regex

        // Order by last_name
        transformedDF = transformedDF.orderBy(transformedDF.col("last_name").desc()); //  rlike() - for regex

        // 5. Show the first three rows
        transformedDF.show(3);

        // 6. Save the data to a database
        String dbConnectionUrl = "jdbc:postgresql://localhost/course_data"; // You need to start postgres and create this database first
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "nbisht");
        prop.setProperty("password", "password"); // The password you used while installing Postgres

        transformedDF.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "project1", prop);

    }


}
