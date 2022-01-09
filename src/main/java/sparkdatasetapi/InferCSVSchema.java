package sparkdatasetapi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {

    public void printSchema() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv") //
                .option("header", "true") //
                .option("multiline", true) // to specify that the value of columns can be multiple lines
                .option("sep", ";") // to specify CSV separator we want to use, here ;
                .option("quote", "^") // to specify the quote being used for strings, here we're using ^ instead of "
                .option("dateFormat", "M/d/y") // to specify the date format
                .option("inferSchema", true) // spark automatically infers the data type of columns if inferSchema is true
                .load("src/main/resources/amazon_products.txt");

        System.out.println("Excerpt of the dataframe content:");
//		    df.show(7);
        df.show(7, 90); // truncate after 90 chars for each column
        System.out.println("Dataframe's schema:");
        df.printSchema();
    }

}
