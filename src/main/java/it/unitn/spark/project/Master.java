package it.unitn.spark.project;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.apache.spark.storage.StorageLevel;

import it.unitn.spark.project.custom_classes.*;
import it.unitn.spark.project.analysis.*;
import it.unitn.spark.project.analysis.DateTimeAnalysis.FromTo;
import scala.Tuple2;

public class Master {
	static File outputDir = new File("." + System.getProperty("file.separator") + "csvOutputFiles");
	
	public static void main(String[] args) throws AnalysisException {
		//time
		long start, end;
		start = System.currentTimeMillis();
		//output dir
		outputDir.mkdirs();
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
		
		JavaRDD<Row> lines = spark.read().format("CSV")
				.option("header", "true")
				//.load("yellow_tripdata_2018-01.csv")
				.load("files/yellow_tripdata_2018-01.csv")
				.persist(StorageLevel.MEMORY_ONLY())	/* https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence */
				.limit(10)
				.javaRDD()
				;
		JavaRDD<Row> lookUpTable = spark.read().format("CSV").option("header", "true").load("taxi _zone_lookup.csv").javaRDD();
		TaxyZone taxyZone = new TaxyZone(lookUpTable);
		DateTimeAnalysis.setTaxyZone(taxyZone);
		Helper.setTaxyZone(taxyZone);
		String str;
		
		/*****************/
		/**Time analysis**/
		/*****************/
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfTimeIntervalsData = DateTimeAnalysis.getValuableDataForTimeIntervals(lines);
//		JavaPairRDD<Integer,Row> reducedListOfTimeIntervalsData = DateTimeAnalysis.getAllReducedData(listOfTimeIntervalsData);
//		str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfTimeIntervalsData, Time_intervals.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "TimeIntervals.csv");
////		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfWeekendWeekdaysData = DateTimeAnalysis.getValuableDataForWeekendWeekdays(lines);
//		JavaPairRDD<Integer,Row> reducedListOfWeekendWeekdaysData = DateTimeAnalysis.getAllReducedData(listOfWeekendWeekdaysData);
//		str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfWeekendWeekdaysData, DayOfWeek.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "WeekendWeekday.csv");
////		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfWWTIData = DateTimeAnalysis.getValuableDataForWWTI(lines);
//		JavaPairRDD<Integer,Row> reducedListOfWWTIData = DateTimeAnalysis.getAllReducedData(listOfWWTIData);
//		str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfWWTIData, DayOfWeek.class, Time_intervals.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "WeekendWeekdayTimeIntervals.csv");
////		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfTIPTData = DateTimeAnalysis.getValuableDataForTIPT(lines);
//		JavaPairRDD<Integer,Row> reducedListOfTIPTData = DateTimeAnalysis.getAllReducedData(listOfTIPTData);
//		str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfTIPTData, Time_intervals.class, Payment_type.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "TimeIntervalsPaymentType.csv");
////		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfWWPTData = DateTimeAnalysis.getValuableDataForWWPT(lines);
//		JavaPairRDD<Integer,Row> reducedListOfWWPTData = DateTimeAnalysis.getAllReducedData(listOfWWPTData);
//		str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfWWPTData, DayOfWeek.class, Payment_type.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "WeekendWeekdayPaymentType.csv");
////		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfWWTIPTData = DateTimeAnalysis.getValuableDataForWWTIPT(lines);
//		JavaPairRDD<Integer,Row> reducedListOfWWTIPTData = DateTimeAnalysis.getAllReducedData(listOfWWTIPTData);
//		str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfWWTIPTData, DayOfWeek.class, Time_intervals.class, Payment_type.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "WeekendWeekdayTimeIntervalsPaymentType.csv");
////		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfTIBData = DateTimeAnalysis.getValuableDataForTIB(lines);
//		JavaPairRDD<Integer,Row> reducedListOfTIBData = DateTimeAnalysis.getAllReducedData(listOfTIBData);
//		str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfTIBData, Time_intervals.class, Boolean.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "TimeIntervalsBorough.csv");
////
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfTIPUData = DateTimeAnalysis.getValuableDataForTIPU(lines);
//		JavaPairRDD<Integer,Row> reducedListOfTIPUData = DateTimeAnalysis.getAllReducedData(listOfTIPUData);
//		str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfTIPUData, Time_intervals.class, FromTo.class, Integer.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "TimeIntervalsPickUpBorough.csv");
////
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfTIPUBData = DateTimeAnalysis.getValuableDataForTIPUB(lines);
//		JavaPairRDD<Integer,Row> reducedListOfTIPUBData = DateTimeAnalysis.getAllReducedData(listOfTIPUBData);
//		str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfTIPUBData, Time_intervals.class, FromTo.class, TaxyZone.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "TimeIntervalsPickUpBorough.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfWWPUBData = DateTimeAnalysis.getValuableDataForWWPUB(lines);
//		JavaPairRDD<Integer,Row> reducedListOfWWPUBData = DateTimeAnalysis.getAllReducedData(listOfWWPUBData);
//		//DateTimeAnalysis.printDataAnalysis(reducedListOfWWPUBData, DayOfWeek.class, FromTo.class, TaxyZone.class);
//		String str = DateTimeAnalysis.cSVDataAnalysis(reducedListOfWWPUBData, DayOfWeek.class, FromTo.class, TaxyZone.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "WeekendWeekdayPickUpBorough.csv");
		
		/**************************/
		/**Trip Distance analysis**/
		/**************************/
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfDistanceIntervalsData = DistanceAnalysis.getValuableDataForDistanceIntervals(lines);
//		JavaPairRDD<Integer,Row> reducedListOfDistanceIntervalsData = DistanceAnalysis.getAllReducedData(listOfDistanceIntervalsData);
//		//Helper.printDataAnalysis(reducedListOfDistanceIntervalsData, Distance_Intervals.class);
//		str = Helper.cSVDataAnalysis(reducedListOfDistanceIntervalsData, Distance_Intervals.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//		printToCSV(str, "Distance_Intervals.csv");
//		
//		//Distance Analysis for different distance intervals wrt recordId
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfDIRIDData = DistanceAnalysis.getValuableDataForDIRID(lines);
//		JavaPairRDD<Integer,Row> reducedListOfDIRIDData = DistanceAnalysis.getAllReducedData(listOfDIRIDData);
//		//Helper.printDataAnalysis(reducedListOfDIRIDData, Distance_Intervals.class, RateCodeID.class);
//		str = Helper.cSVDataAnalysis(reducedListOfDIRIDData, Distance_Intervals.class, RateCodeID.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//		printToCSV(str, "DistanceIntervalsRateCodeID.csv");
//		
//		/*************************/
//		/**	PU and DO Analysis	**/
//		/*************************/
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfPUDOData = PUDOAnalysis.getValuableDataForPUDO(lines);
//		JavaPairRDD<Integer,Row> reducedListOfPUDOData = PUDOAnalysis.getAllReducedData(listOfPUDOData);
//		//Helper.printDataAnalysis(reducedListOfPUDOData, TaxyZone.class, TaxyZone.class);
//		str = Helper.cSVDataAnalysis(reducedListOfPUDOData, TaxyZone.class, TaxyZone.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//		printToCSV(str, "PickUpDropOff.csv");
////		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfSamePUDOData = PUDOAnalysis.getValuableDataForSamePUDO(lines);
//		JavaPairRDD<Integer,Row> reducedListOfSamePUDOData = PUDOAnalysis.getAllReducedData(listOfSamePUDOData);
//		//Helper.printDataAnalysis(reducedListOfSamePUDOData, TaxyZone.class, TaxyZone.class);
//		str = Helper.cSVDataAnalysis(reducedListOfSamePUDOData, TaxyZone.class, TaxyZone.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//		printToCSV(str, "PickUpDropOff.csv");
//		
//		/*************************/
//		/**	Rate Code Analysis	**/
//		/*************************/
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfRCIDData = RateCodeAnalysis.getValuableDataForRCID(lines);
//		JavaPairRDD<Integer,Row> reducedListOfRCIDData = RateCodeAnalysis.getAllReducedData(listOfRCIDData);
//		//Helper.printDataAnalysis(reducedListOfRCIDData, RateCodeID.class);
//		str = Helper.cSVDataAnalysis(reducedListOfRCIDData, RateCodeID.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//		printToCSV(str, "RateCodeID.csv");
//		
//		/*********************************/
//		/**	Store And Forward Analysis	**/
//		/*********************************/
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfSFData = StoreForwardAnalysis.getValuableDataForSF(lines);
//		JavaPairRDD<Integer,Row> reducedListOfSFData = StoreForwardAnalysis.getAllReducedData(listOfSFData);
//		//Helper.printDataAnalysis(reducedListOfSFData, StoreForward.class);
//		str = Helper.cSVDataAnalysis(reducedListOfSFData, StoreForward.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//		printToCSV(str, "StoreForward.csv");
//		
//		/*****************************/
//		/**	Payment Type Analysis	**/
//		/*****************************/
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfPTData = PaymentTypeAnalysis.getValuableDataForPT(lines);
//		JavaPairRDD<Integer,Row> reducedListOfPTData = PaymentTypeAnalysis.getAllReducedData(listOfPTData);
//		//Helper.printDataAnalysis(reducedListOfPTData, Payment_type.class);
//		str = Helper.cSVDataAnalysis(reducedListOfPTData, Payment_type.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "PaymentType.csv");
//		
//		/*****************************/
//		/**	Fare Amount Analysis	**/
//		/*****************************/
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfFAData = FareAmountAnalysis.getValuableDataForFA(lines);
//		JavaPairRDD<Integer,Row> reducedListOfFAData = Helper.getAllReducedData(listOfFAData);
//		//Helper.printDataAnalysis(reducedListOfFAData, Fare_Amount_Intervals.class);
//		str = Helper.cSVDataAnalysis(reducedListOfFAData, Fare_Amount_Intervals.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "FareIntervals.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfFADIData = FareAmountAnalysis.getValuableDataForFADI(lines);
//		JavaPairRDD<Integer,Row> reducedListOfFADIData = Helper.getAllReducedData(listOfFADIData);
//		str = Helper.cSVDataAnalysis(reducedListOfFADIData, Fare_Amount_Intervals.class, Distance_Intervals.class);
//		//Helper.printDataAnalysis(reducedListOfFADIData, Fare_Amount_Intervals.class, Distance_Intervals.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "FareIntervalDistanceInterval.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfFATIData = FareAmountAnalysis.getValuableDataForFATI(lines);
//		JavaPairRDD<Integer,Row> reducedListOfFATIData = Helper.getAllReducedData(listOfFATIData);
//		str = Helper.cSVDataAnalysis(reducedListOfFATIData, Fare_Amount_Intervals.class, Time_intervals.class);
//		//Helper.printDataAnalysis(reducedListOfFATIData, Fare_Amount_Intervals.class, Time_intervals.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "FareIntervalTimeInterval.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Double,Integer> listOfNFAData = FareAmountAnalysis.getValuableDataForNFA(lines);
//		long countTotal = listOfNFAData.count();
//		long countNeg = listOfNFAData.filter(a-> (a._1!=null && a._2!=null)).count();
//		System.out.println("negitive transections: "+ countNeg);
//		System.out.println("total transections: "+ countTotal);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//		
//		/*************************/
//		/**	Vender ID Analysis	**/
//		/*************************/
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfVIDData = VenderIDAnalysis.getValuableDataForVID(lines);
//		JavaPairRDD<Integer,Row> reducedListOfVIDData = Helper.getAllReducedData(listOfVIDData);
//		str = Helper.cSVDataAnalysis(reducedListOfVIDData, VenderID.class);
//		//Helper.printDataAnalysis(reducedListOfVIDData, VenderID.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "Vender.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfVIDDIData = VenderIDAnalysis.getValuableDataForVIDDI(lines);
//		JavaPairRDD<Integer,Row> reducedListOfVIDDIData = Helper.getAllReducedData(listOfVIDDIData);
//		str = Helper.cSVDataAnalysis(reducedListOfVIDDIData, VenderID.class, Distance_Intervals.class);
//		//Helper.printDataAnalysis(reducedListOfVIDDIData, VenderID.class, Distance_Intervals.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "VenderDistanceIntervals.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfVIDTIData = VenderIDAnalysis.getValuableDataForVIDTI(lines);
//		JavaPairRDD<Integer,Row> reducedListOfVIDTIData = Helper.getAllReducedData(listOfVIDTIData);
//		str = Helper.cSVDataAnalysis(reducedListOfVIDTIData, VenderID.class, Time_intervals.class);
//		//Helper.printDataAnalysis(reducedListOfVIDTIData, VenderID.class, Time_intervals.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "VenderTimeIntervals.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfVIDPUData = VenderIDAnalysis.getValuableDataForVIDPU(lines);
//		JavaPairRDD<Integer,Row> reducedListOfVIDPUData = Helper.getAllReducedData(listOfVIDPUData);
//		str = Helper.cSVDataAnalysis(reducedListOfVIDPUData, VenderID.class, TaxyZone.class);
//		//Helper.printDataAnalysis(reducedListOfVIDPUData, VenderID.class, TaxyZone.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "VenderPickupZone.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfVIDDOData = VenderIDAnalysis.getValuableDataForVIDDO(lines);
//		JavaPairRDD<Integer,Row> reducedListOfVIDDOData = Helper.getAllReducedData(listOfVIDDOData);
//		str = Helper.cSVDataAnalysis(reducedListOfVIDDOData, VenderID.class, TaxyZone.class);
//		//Helper.printDataAnalysis(reducedListOfVIDDOData, VenderID.class, TaxyZone.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "VenderDropeOffZone.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfVIDPTData = VenderIDAnalysis.getValuableDataForVIDPT(lines);
//		JavaPairRDD<Integer,Row> reducedListOfVIDPTData = Helper.getAllReducedData(listOfVIDPTData);
//		str = Helper.cSVDataAnalysis(reducedListOfVIDPTData, VenderID.class, Payment_type.class);
//		//Helper.printDataAnalysis(reducedListOfVIDPTData, VenderID.class, Payment_type.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "VenderPaymentType.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfVIDSFData = VenderIDAnalysis.getValuableDataForVIDSF(lines);
//		JavaPairRDD<Integer,Row> reducedListOfVIDSFData = Helper.getAllReducedData(listOfVIDSFData);
//		str = Helper.cSVDataAnalysis(reducedListOfVIDSFData, VenderID.class, StoreForward.class);
//		//Helper.printDataAnalysis(reducedListOfVIDSFData, VenderID.class, StoreForward.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "VenderStoreForward.csv");
//		
//		start = System.currentTimeMillis();
//		JavaPairRDD<Integer,Row> listOfVIDWDData = VenderIDAnalysis.getValuableDataForVIDWD(lines);
//		JavaPairRDD<Integer,Row> reducedListOfVIDWDData = Helper.getAllReducedData(listOfVIDWDData);
//		str = Helper.cSVDataAnalysis(reducedListOfVIDWDData, VenderID.class, DayOfWeek.class);
//		//Helper.printDataAnalysis(reducedListOfVIDWDData, VenderID.class, DayOfWeek.class);
//		end = System.currentTimeMillis();
//		System.out.println(getStringTime(start,end));
//	    printToCSV(str, "VenderDayOfWeek.csv");
		
		
		
		//System.out.println(taxyZone.boroughString());
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
	
	private static String getStringTime(long start, long end) {
		long delta = (end-start);
		return "exec time(h:min:sec:ms): " + delta / 1000 / 60 / 60 + ":" + (delta/1000/60) % 60 + ":" + (delta/1000) % 60 + ":" + delta % 1000 + "\n";
	}
	private static void printToCSV(String content, String fileName) {
		Path path = Paths.get(outputDir.getAbsolutePath() + System.getProperty("file.separator") + fileName);
	    byte[] strToBytes = content.getBytes();
	    try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(path))){
	    	out.write(strToBytes, 0, strToBytes.length);
	    } catch (IOException x) {
	    	System.err.println(x);
	    }
	}
}
