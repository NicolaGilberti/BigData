package it.unitn.spark.project.analysis;

import java.text.ParseException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import it.unitn.spark.project.custom_classes.Helper;
import scala.Tuple2;

public class PUDOAnalysis {
	
	

	public static JavaPairRDD<Integer, Row> getValuableDataForPUDO(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForPUDO(a));
		return listWithKey;
	}
	public static JavaPairRDD<Integer, Row> getValuableDataForSamePUDO(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForSamePUDO(a)).filter(a-> (a._1!=null && a._2!=null));
		return listWithKey;
	}

	
	/******************/
	/**		Maps	 **/
	/**
	 * @throws ParseException 
	 * @throws NumberFormatException ****************/
	
	private static Tuple2<Integer,Row> mapDataForPUDO(Row a) throws NumberFormatException, ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		keyPt1 = Helper.taxyZone.getBoroughDistId(Helper.taxyZone.getBorough(Integer.parseInt(a.getAs("PULocationID"))));
		keyPt2 = Helper.taxyZone.getBoroughDistId(Helper.taxyZone.getBorough(Integer.parseInt(a.getAs("DOLocationID"))));
		String keyS = keyPt1 + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	private static Tuple2<Integer,Row> mapDataForSamePUDO(Row a) throws NumberFormatException, ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		keyPt1 = Helper.taxyZone.getBoroughDistId(Helper.taxyZone.getBorough(Integer.parseInt(a.getAs("PULocationID"))));
		keyPt2 = Helper.taxyZone.getBoroughDistId(Helper.taxyZone.getBorough(Integer.parseInt(a.getAs("DOLocationID"))));
		if(!keyPt1.equals(keyPt2))
			return new Tuple2<Integer,Row>(null,null);
		String keyS = keyPt1 + "" + keyPt2;
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
	

}
