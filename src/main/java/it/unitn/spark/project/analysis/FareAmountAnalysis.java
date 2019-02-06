package it.unitn.spark.project.analysis;

import java.text.ParseException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import it.unitn.spark.project.custom_classes.Distance_Intervals;
import it.unitn.spark.project.custom_classes.Fare_Amount_Intervals;
import it.unitn.spark.project.custom_classes.Helper;
import scala.Tuple2;

public class FareAmountAnalysis {

	public static JavaPairRDD<Integer, Row> getValuableDataForFA(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForFareAmount(a));
		return listWithKey;
	}
	public static JavaPairRDD<Integer, Row> getValuableDataForFADI(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForFADI(a));
		return listWithKey;
	}
	
	
	/******************/
	/**		Maps	 **/
	/******************/
	/* @throws ParseException */
	private static Tuple2<Integer,Row> mapDataForFareAmount(Row a) throws ParseException {
		Integer key =0;
		Row value;
		for(int i=0; i< Fare_Amount_Intervals.values().length; i++) {
			if(inInterval(a, Fare_Amount_Intervals.values()[i])) {
				key = i;
			}
		}
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer, Row>((key+1),value);
	}
	
	/* @throws ParseException */
	private static Tuple2<Integer,Row> mapDataForFADI(Row a) throws ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		for(int i=0; i< Fare_Amount_Intervals.values().length; i++) {
			if(inInterval(a, Fare_Amount_Intervals.values()[i])) {
				keyPt1 = i;
			}
		}
		for(int i=0; i< Distance_Intervals.values().length; i++) {
			if(inInterval(a, Distance_Intervals.values()[i])) {
				keyPt2 = i;
			}
		}
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	

	public static JavaPairRDD<Integer, Row> getAllReducedData(JavaPairRDD<Integer, Row> fullList) {
		JavaPairRDD<Integer,Row> listWithAvg = null;
		if(fullList.isEmpty()) {
			throw new java.lang.RuntimeException("Empty RDD");
		}
		listWithAvg = fullList.reduceByKey((a,b) -> Helper.counts(a,b));
		return listWithAvg;
	}
	
	/*********************/
	/**		Filters		**/
	/*********************/
	private static boolean inInterval(Row a, Fare_Amount_Intervals d) throws ParseException {
		Float toCheck = Float.parseFloat(a.getAs("trip_distance"));
		if(toCheck >= d.getStartInterval() && 
				toCheck < d.getEndInterval()) {
			return true;
		}
		return false;
	}
	
	private static boolean inInterval(Row a, Distance_Intervals d) throws ParseException {
		Float toCheck = Float.parseFloat(a.getAs("trip_distance"));
		if(toCheck >= d.getStartInterval() && 
				toCheck < d.getEndInterval()) {
			return true;
		}
		return false;
	}

}
