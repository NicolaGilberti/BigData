package it.unitn.spark.project.analysis;

import java.text.ParseException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import it.unitn.spark.project.custom_classes.DayOfWeek;
import it.unitn.spark.project.custom_classes.Distance_Intervals;
import it.unitn.spark.project.custom_classes.Helper;
import it.unitn.spark.project.custom_classes.StoreForward;
import it.unitn.spark.project.custom_classes.Time_intervals;
import scala.Tuple2;

public class VenderIDAnalysis {

	public static JavaPairRDD<Integer, Row> getValuableDataForVIDDI(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForVIDDI(a));
		return listWithKey;
	}
	public static JavaPairRDD<Integer, Row> getValuableDataForVIDTI(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForVIDTI(a));
		return listWithKey;
	}

	public static JavaPairRDD<Integer, Row> getValuableDataForVIDPU(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForVIDPU(a));
		return listWithKey;
	}
	public static JavaPairRDD<Integer, Row> getValuableDataForVIDDO(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForVIDDO(a));
		return listWithKey;
	}
	public static JavaPairRDD<Integer, Row> getValuableDataForVIDPT(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForVIDPT(a));
		return listWithKey;
	}

	public static JavaPairRDD<Integer, Row> getValuableDataForVIDSF(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForVIDSF(a));
		return listWithKey;
	}
	public static JavaPairRDD<Integer, Row> getValuableDataForVIDWD(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForVIDWD(a));
		return listWithKey;
	}
	public static JavaPairRDD<Integer, Row> getValuableDataForVID(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForVID(a));
		return listWithKey;
	}
	
	/******************/
	/**		Maps	 **/
	/******************/
	/* @throws ParseException 
	 * @throws NumberFormatException */

	private static Tuple2<Integer,Row> mapDataForVID(Row a) throws NumberFormatException, ParseException {
		Integer key = 0;
		Row value = null;
		key = Integer.parseInt(a.getAs("VendorID"));
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	/* @throws ParseException */
	private static Tuple2<Integer,Row> mapDataForVIDDI(Row a) throws ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		keyPt1 = Integer.parseInt(a.getAs("VendorID"))-1;
		for(int i=0; i< Distance_Intervals.values().length; i++) {
			if(Helper.inInterval(a, Distance_Intervals.values()[i])) {
				keyPt2 = i;
			}
		}
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	/* @throws ParseException */
	private static Tuple2<Integer,Row> mapDataForVIDTI(Row a) throws ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		keyPt1 = Integer.parseInt(a.getAs("VendorID"))-1;
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(Helper.inInterval(a, Time_intervals.values()[i])) {
				keyPt2 = i;
			}
		}
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	private static Tuple2<Integer,Row> mapDataForVIDPU(Row a) throws NumberFormatException, ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		keyPt1 = Integer.parseInt(a.getAs("VendorID"))-1;
		keyPt2 = Helper.taxyZone.getBoroughDistId(Helper.taxyZone.getBorough(Integer.parseInt(a.getAs("PULocationID"))))-1;
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	private static Tuple2<Integer,Row> mapDataForVIDDO(Row a) throws NumberFormatException, ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		keyPt1 = Integer.parseInt(a.getAs("VendorID"))-1;
		keyPt2 = Helper.taxyZone.getBoroughDistId(Helper.taxyZone.getBorough(Integer.parseInt(a.getAs("DOLocationID"))))-1;
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	private static Tuple2<Integer,Row> mapDataForVIDPT(Row a) throws NumberFormatException, ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		keyPt1 = Integer.parseInt(a.getAs("VendorID"))-1;
		keyPt2 = Integer.parseInt(a.getAs("payment_type"))-1;
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	private static Tuple2<Integer,Row> mapDataForVIDSF(Row a) throws NumberFormatException, ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 1;
		Row value = null;
		keyPt1 = Integer.parseInt(a.getAs("VendorID"))-1;
		if(a.getAs("store_and_fwd_flag").equals(StoreForward.values()[0].getStatus()))
			keyPt2 =0;
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	private static Tuple2<Integer,Row> mapDataForVIDWD(Row a) throws NumberFormatException, ParseException {
		Integer keyPt1 = 0;
		Integer keyPt2 = 1;
		Row value = null;
		keyPt1 = Integer.parseInt(a.getAs("VendorID"))-1;
		if(Helper.weekEnd(a)) {
			keyPt2 = DayOfWeek.WEEKEND.ordinal();
		}else {
			keyPt2 = DayOfWeek.WEEKDAY.ordinal();
		}
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = Helper.getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}

}
