package it.unitn.spark.project.analysis;

import java.text.ParseException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import it.unitn.spark.project.custom_classes.Helper;
import scala.Tuple2;

public class PaymentTypeAnalysis {

	public static JavaPairRDD<Integer, Row> getValuableDataForPT(JavaRDD<Row> fullList) {
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForPaymentType(a));
		return listWithKey;
	}

	/******************/
	/**		Maps	 **/
	/**
	 * @throws ParseException 
	 * @throws NumberFormatException ****************/
	
	 /* @throws ParseException */
	private static Tuple2<Integer,Row> mapDataForPaymentType(Row a) throws NumberFormatException, ParseException {
		Integer key = 0;
		Row value;
		key = Integer.parseInt(a.getAs("payment_type"));
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
