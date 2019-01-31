package it.unitn.spark.project.analysis;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import it.unitn.spark.project.custom_classes.Helper;
import it.unitn.spark.project.custom_classes.RateCodeID;
import scala.Tuple2;

public class RateCodeAnalysis {

	public static JavaPairRDD<Integer, Row> getValuableDataForRCID(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForRCID(a));
		return listWithKey;
	}
	
	/******************/
	/**		Maps	 **/
	/******************/

	private static Tuple2<Integer,Row> mapDataForRCID(Row a) {
		Integer keyPt1 = 0;
		Row value = null;
		for(int i=0; i< RateCodeID.values().length; i++) {
			if(rateCodeType(a, RateCodeID.values()[i])) {
				keyPt1 = i;
			}
		}
		String keyS = (keyPt1+1) + "" + (keyPt1+1);
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	/*********************/
	/**		Reduces		**/
	/*********************/
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
	public static boolean rateCodeType(Row a, RateCodeID rid) {
		if(Integer.parseInt(a.getAs("RatecodeID")) == rid.getRealTag()) {
			return true;
		}
		return false;
	}


}
