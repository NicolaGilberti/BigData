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
		
		JavaRDD<Row> lines = spark.read().format("CSV").option("header", "true").load("yellow_tripdata_2018-01.csv").javaRDD();//temp
		/*****************/
		/**Time analysis**/
		/*****************/
//		long a = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfTimeIntervalsData = DateTimeAnalysis.getValuableDataForTimeIntervals(lines);
//		JavaPairRDD<Integer,Row> reducedListOfTimeIntervalsData = DateTimeAnalysis.getAllAverages(listOfTimeIntervalsData);
//		Iterator<Tuple2<Integer, Row>> timeIntervalsIt= reducedListOfTimeIntervalsData.collect().iterator();
//		while(timeIntervalsIt.hasNext()) {
//			Tuple2<Integer, Row> tup  = timeIntervalsIt.next();
//			Time_intervals ti = Time_intervals.values()[tup._1];
//			String formattedRow ="";
//			MaxValueManager maxPassengerCount = tup._2.getAs(0);
//			MaxValueManager maxTrip = tup._2.getAs(1);
//			MaxValueManager maxExtra = tup._2.getAs(2);
//			MaxValueManager maxTip = tup._2.getAs(3);
//			MaxValueManager maxTotal = tup._2.getAs(4);
//			Integer passengerCounter = tup._2.getAs(5);
//			Float sumTrip = tup._2.getAs(6);
//			Float sumTip = tup._2.getAs(7);
//			Float sumTotal = tup._2.getAs(8);
//			Integer counter = tup._2.getAs(9);
//			formattedRow += "\tMaxPassengerCount: " + maxPassengerCount + "\n";
//			formattedRow += "\tMaxTripDistance: " + maxTrip + "\n";
//			formattedRow += "\tMaxExtraPaid: " + maxExtra + "\n";
//			formattedRow += "\tMaxTipPaid: " + maxTip + "\n";
//			formattedRow += "\tMaxTotalPaid: " + maxTotal + "\n";
//			formattedRow += "\tAvgPassengerCounter: " + ((double)passengerCounter / counter*1.0) + "\n";
//			formattedRow += "\tAvgTripDistance: " + ((double)sumTrip / counter*1.0) + "\n";
//			formattedRow += "\tAvgTipPaid: " + ((double)sumTip / counter*1.0) + "\n";
//			formattedRow += "\tAvgTotalPaid: " + ((double)sumTotal / counter*1.0) + "\n";
//			formattedRow += "\tRowEvaluated: " + counter + "\n";
//			System.out.println(ti.getClass().getSimpleName() + ": " + ti + " results:\n" + formattedRow);
//		}
//		long b = System.currentTimeMillis();
//		long deltaAB = (b-a);
//		System.out.println("exec time(h:min:sec:ms): " + deltaAB / 1000 / 60 / 60 + ":" + (deltaAB/1000/60) % 60 + ":" + (deltaAB/1000) % 60 + ":" + deltaAB % 1000 + "\n");

//		long c = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfWeekendWeekdaysData = DateTimeAnalysis.getValuableDataForWeekendWeekdays(lines);
//		JavaPairRDD<Integer,Row> reducedListOfWeekendWeekdaysData = DateTimeAnalysis.getAllAverages(listOfWeekendWeekdaysData);
//		Iterator<Tuple2<Integer, Row>> weekendWeekdaysIt= reducedListOfWeekendWeekdaysData.collect().iterator();
//		while(weekendWeekdaysIt.hasNext()) {
//			Tuple2<Integer, Row> tup  = weekendWeekdaysIt.next();
//			DayOfWeek ti = DayOfWeek.values()[tup._1];
//			String formattedRow ="";
//			MaxValueManager maxPassengerCount = tup._2.getAs(0);
//			MaxValueManager maxTrip = tup._2.getAs(1);
//			MaxValueManager maxExtra = tup._2.getAs(2);
//			MaxValueManager maxTip = tup._2.getAs(3);
//			MaxValueManager maxTotal = tup._2.getAs(4);
//			Integer passengerCounter = tup._2.getAs(5);
//			Float sumTrip = tup._2.getAs(6);
//			Float sumTip = tup._2.getAs(7);
//			Float sumTotal = tup._2.getAs(8);
//			Integer counter = tup._2.getAs(9);
//			formattedRow += "\tMaxPassengerCount: " + maxPassengerCount + "\n";
//			formattedRow += "\tMaxTripDistance: " + maxTrip + "\n";
//			formattedRow += "\tMaxExtraPaid: " + maxExtra + "\n";
//			formattedRow += "\tMaxTipPaid: " + maxTip + "\n";
//			formattedRow += "\tMaxTotalPaid: " + maxTotal + "\n";
//			formattedRow += "\tAvgPassengerCounter: " + ((double)passengerCounter / counter*1.0) + "\n";
//			formattedRow += "\tAvgTripDistance: " + ((double)sumTrip / counter*1.0) + "\n";
//			formattedRow += "\tAvgTipPaid: " + ((double)sumTip / counter*1.0) + "\n";
//			formattedRow += "\tAvgTotalPaid: " + ((double)sumTotal / counter*1.0) + "\n";
//			formattedRow += "\tRowEvaluated: " + counter + "\n";
//			System.out.println(ti.getClass().getSimpleName() + ": " + ti + " results:\n" + formattedRow);
//		}
//		long d = System.currentTimeMillis();
//		long deltaCD = (d-c);
//		System.out.println("exec time(h:min:sec:ms): " + deltaTCD / 1000 / 60 / 60 + ":" + (deltaCD/1000/60) % 60 + ":" + (deltaCD/1000) % 60 + ":" + deltaCD % 1000 + "\n");

		long e = System.currentTimeMillis();
		JavaPairRDD<Integer,Row> listOfWWTIData = DateTimeAnalysis.getValuableDataForWWTI(lines);
		JavaPairRDD<Integer,Row> reducedListOfWWTIData = DateTimeAnalysis.getAllAverages(listOfWWTIData);
		Iterator<Tuple2<Integer, Row>> WWTIIt= reducedListOfWWTIData.collect().iterator();
		while(WWTIIt.hasNext()) {
			Tuple2<Integer, Row> tup  = WWTIIt.next();
			int key = tup._1;
			String keyS = "";
			DayOfWeek ww = DayOfWeek.values()[key/10];
			Time_intervals ti = Time_intervals.values()[key%10];
			keyS += ww.getClass().getSimpleName() + ": " + ww + " ";
			keyS += ti.getClass().getSimpleName() + ": " + ti  + "\nResults:\n" ;
			String formattedRow ="";
			MaxValueManager maxPassengerCount = tup._2.getAs(0);
			MaxValueManager maxTrip = tup._2.getAs(1);
			MaxValueManager maxExtra = tup._2.getAs(2);
			MaxValueManager maxTip = tup._2.getAs(3);
			MaxValueManager maxTotal = tup._2.getAs(4);
			Integer passengerCounter = tup._2.getAs(5);
			Float sumTrip = tup._2.getAs(6);
			Float sumTip = tup._2.getAs(7);
			Float sumTotal = tup._2.getAs(8);
			Integer counter = tup._2.getAs(9);
			formattedRow += "\tMaxPassengerCount: " + maxPassengerCount + "\n";
			formattedRow += "\tMaxTripDistance: " + maxTrip + "\n";
			formattedRow += "\tMaxExtraPaid: " + maxExtra + "\n";
			formattedRow += "\tMaxTipPaid: " + maxTip + "\n";
			formattedRow += "\tMaxTotalPaid: " + maxTotal + "\n";
			formattedRow += "\tAvgPassengerCounter: " + ((double)passengerCounter / counter*1.0) + "\n";
			formattedRow += "\tAvgTripDistance: " + ((double)sumTrip / counter*1.0) + "\n";
			formattedRow += "\tAvgTipPaid: " + ((double)sumTip / counter*1.0) + "\n";
			formattedRow += "\tAvgTotalPaid: " + ((double)sumTotal / counter*1.0) + "\n";
			formattedRow += "\tRowEvaluated: " + counter + "\n";
			System.out.println(keyS + formattedRow);
		}
		long f = System.currentTimeMillis();
		long deltaEF = (f-e);
		System.out.println("exec time(h:min:sec:ms): " + deltaEF / 1000 / 60 / 60 + ":" + (deltaEF/1000/60) % 60 + ":" + (deltaEF/1000) % 60 + ":" + deltaEF % 1000 + "\n");

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
