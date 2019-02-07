package it.unitn.spark.project.analysis;

import java.text.ParseException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import it.unitn.spark.project.custom_classes.Helper;
import it.unitn.spark.project.custom_classes.StoreForward;
import scala.Tuple2;

public class StoreForwardAnalysis {

	public static JavaPairRDD<Integer, Row> getValuableDataForSF(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForSF(a));
		return listWithKey;
	}

	private static Tuple2<Integer,Row> mapDataForSF(Row a) throws NumberFormatException, ParseException {
		Integer key = 2;
		Row value = null;
		if(a.getAs("store_and_fwd_flag").equals(StoreForward.values()[0].getStatus()))
			key =1;
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

}
