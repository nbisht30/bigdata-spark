package datasetvsdataframe.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class  ArrayToDataset {

	public void start() {
		// 1. Create spark session
		SparkSession spark = new SparkSession.Builder()
				.appName("Array To Dataset<String>")
				.master("local")
				.getOrCreate();

		// 2. Creating a list of strings
		String [] stringList = new String[] {"Banana", "Car", "Glass", "Banana", "Computer", "Car"};
		List<String> data = Arrays.asList(stringList);

		// 3. Use createDataset() to create the dataset of type String
		// Envoders.STRING() will be the data type recognised by Spark for the elements in data list
		Dataset<String> ds =  spark.createDataset(data, Encoders.STRING());

		// 4. Checking the schema
		ds.printSchema();
		ds.show(10);

		// 5. Group by(Converts the dataset to dataframe as these methods are supported by df only)
		Dataset<Row> df = ds.groupBy("value").count();
		df.show();

		// 6. Another way of converting a dataset to a dataframe
		Dataset<Row> df2 = ds.toDF();

		// 7. Converting dataframe to a dataset
		Dataset<String> dsFromDf = df.as(Encoders.STRING());

//		 ds = ds.map((MapFunction<String, String>) row -> "word: " + row, Encoders.STRING());
//		 ds.show(10);
//
//		String stringValue = ds.reduce(new StringReducer());
//
//		System.out.println(stringValue);
		
	}
	
	
	static class StringReducer implements ReduceFunction<String>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public String call(String v1, String v2) throws Exception {
			return v1 + v2;
		}
		
	}

}
