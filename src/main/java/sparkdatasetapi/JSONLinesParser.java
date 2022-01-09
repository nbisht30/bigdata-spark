package sparkdatasetapi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLinesParser {


    public void parseJsonLines() {
        SparkSession spark = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local")
                .getOrCreate();

        System.out.println("Singleline JSON: ");
        // Singleline JSON Parsing
        Dataset<Row> singlelineDF = spark.read().format("json") // json -> to read json files
                .load("src/main/resources/simple.json");
        singlelineDF.show(5, 150);
        System.out.println("Dataframe's schema:");
        singlelineDF.printSchema();

        System.out.println("Multiline JSON: ");
        // Multiline JSON Parsing
        Dataset<Row> multilineDF = spark.read().format("json")
                .option("multiline", true)
                .load("src/main/resources/multiline.json");

        multilineDF.show(5, 150);
        System.out.println("Dataframe's schema:");
        multilineDF.printSchema();

    }


}



