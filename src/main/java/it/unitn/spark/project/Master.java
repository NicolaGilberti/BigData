package it.unitn.spark.project;

import java.io.File;
import java.util.Date;
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

import it.unitn.spark.project.custom_classes.*;
import it.unitn.spark.project.datetime.DateTimeAnalysis;
import scala.Tuple2;

public class Master {
	public static void main(String[] args) throws AnalysisException {
		// SparkSession
		Builder builder = new Builder().appName("SparkSQL Examples");
		//if (new File("/Users/").exists()) {
			builder.master("local");
		//}
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
		
		JavaRDD<Row> lines = spark.read().format("CSV").option("header", "true").load("temp.csv").javaRDD();//temp
		/*****************/
		/**Time analysis**/
		/*****************/
		long c = System.currentTimeMillis();
		JavaPairRDD<Integer,Row> listofData = DateTimeAnalysis.getValuableDataForTimeIntervals(lines);
		JavaPairRDD<Integer,Row> redListofData = DateTimeAnalysis.getAllAverages(listofData);
		Iterator<Tuple2<Integer, Row>> it= redListofData.collect().iterator();
		while(it.hasNext()) {
			Tuple2<Integer, Row> tup  = it.next();
			Time_intervals ti = Time_intervals.values()[tup._1];
			String formattedRow ="";
			MaxValueManager maxTrip = tup._2.getAs(0);
			MaxValueManager maxExtra = tup._2.getAs(1);
			MaxValueManager maxTip = tup._2.getAs(2);
			MaxValueManager maxTotal = tup._2.getAs(3);
			Float sumTrip = tup._2.getAs(4);
			Float sumTip = tup._2.getAs(5);
			Float sumTotal = tup._2.getAs(6);
			Integer counter = tup._2.getAs(7);
			formattedRow += "\t[maxTripDistance: " + maxTrip + "]\n";
			formattedRow += "\t[maxExtraPaid: " + maxExtra + "]\n";
			formattedRow += "\t[maxTipPaid: " + maxTip + "]\n";
			formattedRow += "\t[maxTotalPaid: " + maxTotal + "]\n";
			formattedRow += "\t[AvgTripDistance: " + ((double)sumTrip / counter*1.0) + "]\n";
			formattedRow += "\t[AvgTipPaid: " + ((double)sumTip / counter*1.0) + "]\n";
			formattedRow += "\t[AvgTotalPaid: " + ((double)sumTotal / counter*1.0) + "]\n";
			formattedRow += "\t[RowEvaluated: " + counter + "]\n";
			System.out.println("Time: " + ti + " results:\n" + formattedRow);
		}
		long d = System.currentTimeMillis();
		long deltaT = (d-c);
		System.out.println("exec time(h:min:sec:ms): " + deltaT / 1000 / 60 / 60 + ":" + (deltaT/1000/60) % 60 + ":" + (deltaT/1000) % 60 + ":" + deltaT % 1000);
//		System.out.println("weekday/weekend:");
//		System.out.println(DateTimeAnalysis.getAveragePassenger(weekDay));
//		System.out.println(DateTimeAnalysis.getAveragePassenger(weekEnd));
//		System.out.println("Extra check:");
//		System.out.println(DateTimeAnalysis.getAveragePassenger(DateTimeAnalysis.getDateTimeIntervals(weekEnd,Time_intervals.NIGHT)));
//		System.out.println(DateTimeAnalysis.getMaxPassengerCount(DateTimeAnalysis.getDateTimeIntervals(weekEnd,Time_intervals.MORNING)));
//		System.out.println(DateTimeAnalysis.getMaxPassengerCount(DateTimeAnalysis.getDateTimeIntervals(weekEnd,Time_intervals.AFTERNOON)));
//		System.out.println(DateTimeAnalysis.getMaxPassengerCount(DateTimeAnalysis.getDateTimeIntervals(weekEnd,Time_intervals.NIGHT)));
//		System.out.println(DateTimeAnalysis.getMaxPassengerCount(DateTimeAnalysis.getDateTimeIntervals(weekDay,Time_intervals.MORNING)));
//		System.out.println(DateTimeAnalysis.getMaxPassengerCount(DateTimeAnalysis.getDateTimeIntervals(weekDay,Time_intervals.AFTERNOON)));
//		System.out.println(DateTimeAnalysis.getMaxPassengerCount(DateTimeAnalysis.getDateTimeIntervals(weekDay,Time_intervals.NIGHT)));
//		String date = "2018-01-01 00:00:00";
//		System.out.println(DateTimeAnalysis.getMaxPassengerCount(DateTimeAnalysis.getDateTimePerSpecificWeek(lines, date)));
//		String dateFrom = "2018-01-01 00:00:00";
//		String dateTo = "2018-01-02 00:00:00";
//		System.out.println(DateTimeAnalysis.getMaxPassengerCount(DateTimeAnalysis.getDateTimePerSpecificRange(lines, dateFrom, dateTo)));



		/** trials **/
//		Iterator it= _.collect().iterator();
//		while(it.hasNext()) {
//			System.out.println(it.next().toString());
//		}
		
//		System.out.println(map.reduce((a,b) -> (a>b ? a : b)));
		//(a._1==b._1)?(a._2>=b._2 ? a._2:b._2) :()
		//JavaPairRDD<String, Float> result = map.filter(a -> a._1.equals("1"));
		//Tuple2<String, String> result2 = result.reduce((a,b) -> getMax(a,b));
		//System.out.println(result2);
//		Iterator it= result.collect().iterator();
//		while(it.hasNext()) {
//			System.out.println(it.next().toString());
//		}
		//System.out.println(result.reduce((a,b) -> new Tuple2("res",(Float.max(a._2,b._2)))).toString());
		
		// Untyped Dataset Operationss
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

	private static Tuple2<String, Float> getMax(Tuple2<String, Float> a, Tuple2<String, Float> b) {
		if(a._2>=b._2) {
			return a;
		}
		else {
			return b;
		}
	}

}
