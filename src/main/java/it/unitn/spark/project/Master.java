package it.unitn.spark.project;

import java.io.File;
import java.util.Iterator;

import static org.apache.spark.sql.functions.col;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import scala.Tuple2;

public class Master {
	public static void main(String[] args) throws AnalysisException {
		// SparkSession
		Builder builder = new Builder().appName("SparkSQL Examples");
		if (new File("/Users/").exists()) {
			builder.master("local");
		}
		SparkSession spark = builder.getOrCreate();

		// Obtain JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		//JavaRDD<String> lines = sc.textFile("files/yellow_tripdata_2018-01.csv");
		//System.out.println(lines.toString());

		// Creating DataFrames
		//Dataset<Row> df = spark.read().format("CSV").option("header", "true").load("files/yellow_tripdata_2018-01.csv")
				//.limit(1000)
				;
		//df.show();
		
		JavaRDD<Row> lines = spark.read().format("CSV").option("header", "true").load("files/yellow_tripdata_2018-01.csv").javaRDD();
		JavaPairRDD<String, String> map = lines.mapToPair(s -> new Tuple2<String, String>(s.getAs("VendorID"), s.getAs("total_amount")));
		//Iterator it= map.collect().iterator();
//		while(it.hasNext()) {
//			System.out.println(it.next().toString());
//		}
		//(a._1==b._1)?(a._2>=b._2 ? a._2:b._2) :()
		Tuple2<String, String> result = map.filter(a -> a._1.equals("2")).reduce((a,b) -> getMax(a,b));
		System.out.println(result);
//		Iterator it= result.collect().iterator();
//		while(it.hasNext()) {
//			System.out.println(it.next().toString());
//		}
		//map.reduce((a,b) -> getMax(a,b));
		
		// Untyped Dataset Operations
		//df.printSchema();

		//df.filter(col("total_amount").lt("100.00"))
			//.filter(col("total_amount").gt("0.0"))
		//	.orderBy(col("total_amount").desc())
		//.show();
		
		
		
//		df.createGlobalTempView("sqlData");
//		
//		spark.sql("SELECT VendorID,MAX(total_amount) AS MAX_AMOUNT FROM global_temp.sqlData GROUP BY VendorID").show();
//		
//		spark.sql("SELECT VendorID,MIN(total_amount) AS MIN_AMOUNT FROM global_temp.sqlData GROUP BY VendorID").show();
//		
//		for(int i=-50; i<100; i+=25) {
//			System.out.println(i + " " + ((int)i+25));
//			spark.sql("SELECT * FROM global_temp.sqlData WHERE total_amount BETWEEN "+ i +" AND " + ((int)i+25)).show();
//		}
		
		
		
		
		

		
	}

	private static Tuple2 getMax(Tuple2<String, String> a, Tuple2<String, String> b) {
		if(Double.parseDouble(a._2)>=Double.parseDouble(b._2)) {
			return a;
		}
		else {
			return b;
		}
	}

}
