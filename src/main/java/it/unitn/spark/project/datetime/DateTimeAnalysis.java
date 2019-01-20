package it.unitn.spark.project.datetime;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import it.unitn.spark.project.custom_classes.*;
import scala.Tuple2;

public class DateTimeAnalysis {
	private static Calendar calendar = Calendar.getInstance();
	
//	//test mapwith keys
//	public static void getAvgPerIntervals(JavaRDD<Row> fullList) {
//		JavaPairRDD<Integer, Integer> listWithKeys = fullList.mapToPair(a -> mapp(a,1));
//		JavaPairRDD<Integer, Integer> sumByKey = listWithKeys.reduceByKey((a,b) -> a+b);
//		//troviamo il count per ogni key
//		JavaPairRDD<Integer, Integer> listWithKeys2 = fullList.mapToPair(a -> mapp(a,2));
//		JavaPairRDD<Integer, Integer> countByKey = listWithKeys2.reduceByKey((a,b) -> a+b);
//		Iterator<Tuple2<Integer, Integer>> it = sumByKey.toLocalIterator();
//		Iterator<Tuple2<Integer, Integer>> it2 = countByKey.toLocalIterator();
//		while(it.hasNext() && it2.hasNext()) {
//			Tuple2<Integer, Integer> temp = it.next();
//			Tuple2<Integer, Integer> temp2 = it2.next();
//			System.out.println("Key: " + Time_intervals.values()[temp._1] + " value: " + temp._2*1.0/temp2._2*1.0);
//		}
//	}
	
	public static JavaPairRDD<Integer,Row> getValuableDataForTimeIntervals(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mappingEverything(a));
		return listWithKey;
	}
	public static JavaPairRDD<Integer, Row> getAllAverages(JavaPairRDD<Integer,Row> fullList){
		JavaPairRDD<Integer,Row> listWithAvg = null;
		listWithAvg = fullList.reduceByKey((a,b) -> counts(a,b));
		return listWithAvg;
	}

	/**
	 * find the RDD that return all the row that have the starting time that belong to the interval
	 * @param fullList
	 * @param t
	 * @return
	 */
	public static JavaRDD<Row> getDateTimeIntervals(JavaRDD<Row> fullList, Time_intervals t) {
		JavaRDD<Row> list = fullList.filter(a -> interval(a, t));
		return list;
	}
	/**
	 * find the RDD of all the row that have as starting time a weekday.
	 * @param fullList
	 * @return
	 */
	public static JavaRDD<Row> getDateTimePerWeekDay(JavaRDD<Row> fullList){
		JavaRDD<Row> list = fullList.filter(a -> weekDay(a));
		return list;
	}
	/**
	 * find the RDD of all the row that have as starting time a weekend.
	 * @param fullList
	 * @return
	 */
	public static JavaRDD<Row> getDateTimePerWeekEnd(JavaRDD<Row> fullList){
		JavaRDD<Row> list = fullList.filter(a -> weekEnd(a));
		return list;
	}
	/**
	 * find the RDD of all the row that are in the specific date
	 * @param fullList
	 * @param target
	 * @return
	 */
	public static JavaRDD<Row> getDateTimePerSpecificDay(JavaRDD<Row> fullList, String target){
		JavaRDD<Row> list = fullList.filter(a -> isRowDateInSpecificDay(a,target));
		return list;
	}
	/**
	 * find the RDD of all the row that are in the specific date
	 * @param fullList
	 * @param target
	 * @return
	 */
	public static JavaRDD<Row> getDateTimePerSpecificWeek(JavaRDD<Row> fullList, String target){
		JavaRDD<Row> list = fullList.filter(a -> isRowDateInSpecificWeek(a,target));
		return list;
	}
	/**
	 * find the RDD of all the row that are in the specific date
	 * @param fullList
	 * @param target
	 * @return
	 */
	public static JavaRDD<Row> getDateTimePerSpecificMonth(JavaRDD<Row> fullList, String target){
		JavaRDD<Row> list = fullList.filter(a -> isRowDateInSpecificMonth(a,target));
		return list;
	}
	/**
	 * find the RDD of all the row that are between the specific dates
	 * @param fullList
	 * @param target
	 * @return
	 */
	public static JavaRDD<Row> getDateTimePerSpecificRange(JavaRDD<Row> fullList, String fromDate, String toDate){
		JavaRDD<Row> list = fullList.filter(a -> isRowDateInSpecificRange(a,fromDate,toDate));
		return list;
	}
	
	
	
	
	/**
	 * return the Counter of the passenger for the given list
	 * @param fullList
	 * @param t
	 * @return
	 */
	public static Integer getPassengerCount(JavaRDD<Row> fullList){
		JavaRDD<Integer> listC = fullList.map(a -> Integer.parseInt(a.getAs("passenger_count")));
		if(listC.isEmpty()) {
			return 0;
		}
		return listC.reduce((a,b) -> a+b);
	}
	/**
	 * return the max number of passenger for the given list
	 * @param fullList
	 * @return a tuple that contain the max number and the count of its appearances
	 */

	public static Tuple2<Integer,Integer> getMaxPassengerCount(JavaRDD<Row> fullList){
		JavaRDD<Tuple2<Integer,Integer>> listC = fullList.map(a -> new Tuple2<Integer,Integer>(Integer.parseInt(a.getAs("passenger_count")),1));
		if(listC.isEmpty()) {
			return new Tuple2<Integer, Integer>(0,0);
		}
		return listC.reduce((a,b) -> a._1>b._1?a:(a._1<b._1?b:new Tuple2<Integer,Integer>(a._1,a._2+b._2)));
	}
	/**
	 * return the Counter of the trip for the given list
	 * @param fullList
	 * @param t
	 * @return
	 */
	public static Integer getTripCount(JavaRDD<Row> fullList){
		JavaRDD<Integer> listC = fullList.map(a -> 1);
		if(listC.isEmpty()) {
			return 0;
		}
		return listC.reduce((a,b) -> a+b);
		//same as fullList.count();
	}
	/**
	 * return the average of the passenger for the given list
	 * @param fullList
	 * @param t
	 * @return
	 */
	public static double getAveragePassenger(JavaRDD<Row> fullList){
		double avg=0;
		Integer count = getPassengerCount(fullList);
		Integer ntrips = getTripCount(fullList);
		avg = ((count*1.0)/(ntrips*1.0));
		return avg;
	}
	/*********************/
	/**		Filters		**/
	/*********************/
	private static Boolean weekDay(Row a) throws ParseException {
		Date dateA = getDate(a.getAs("tpep_pickup_datetime"));
		calendar.setTime(dateA);
		int toCheck = calendar.get(Calendar.DAY_OF_WEEK);
		if(toCheck != Calendar.SATURDAY &&
				toCheck != Calendar.SUNDAY) {
			return true;
		}
		return false;
	}
	
	private static Boolean weekEnd(Row a) throws ParseException {
		Date dateA = getDate(a.getAs("tpep_pickup_datetime"));
		calendar.setTime(dateA);
		int toCheck = calendar.get(Calendar.DAY_OF_WEEK);
		if(toCheck == Calendar.SATURDAY ||
				toCheck == Calendar.SUNDAY) {
			return true;
		}
		return false;
	}

	@SuppressWarnings("deprecation")
	private static boolean interval(Row a, Time_intervals t) throws ParseException {
		Date toCheck = getDate(a.getAs("tpep_pickup_datetime"));
		if(t.getStartTime().getHours() > t.getEndTime().getHours()) {
			if(toCheck.getHours() >= t.getStartTime().getHours() || 
					toCheck.getHours() < t.getEndTime().getHours()) {
				return true;
			}
		}else {
			if(toCheck.getHours() >= t.getStartTime().getHours() && 
					toCheck.getHours() < t.getEndTime().getHours()) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isRowDateInSpecificDay(Row a, String target) throws ParseException {
		Date date1 = getDate(a.getAs("tpep_pickup_datetime"));
		Calendar currentCalendar = Calendar.getInstance();
		currentCalendar.setTime(getDate(target));
		int week = currentCalendar.get(Calendar.DAY_OF_YEAR);
		int year = currentCalendar.get(Calendar.YEAR);
		Calendar targetCalendar = Calendar.getInstance();
		targetCalendar.setTime(date1);
		int targetWeek = targetCalendar.get(Calendar.DAY_OF_YEAR);
		int targetYear = targetCalendar.get(Calendar.YEAR);
		return week == targetWeek && year == targetYear;
	}
	
	public static boolean isRowDateInSpecificWeek(Row a, String target) throws ParseException {
		Date date1 = getDate(a.getAs("tpep_pickup_datetime"));
		Calendar currentCalendar = Calendar.getInstance();
		currentCalendar.setTime(getDate(target));
		int week = currentCalendar.get(Calendar.WEEK_OF_YEAR);
		int year = currentCalendar.get(Calendar.YEAR);
		Calendar targetCalendar = Calendar.getInstance();
		targetCalendar.setTime(date1);
		int targetWeek = targetCalendar.get(Calendar.WEEK_OF_YEAR);
		int targetYear = targetCalendar.get(Calendar.YEAR);
		//System.out.println("target: " + targetWeek + " " + targetYear + " input: " + week + " " +year);
		return week == targetWeek && year == targetYear;
	}
	
	public static boolean isRowDateInSpecificMonth(Row a, String target) throws ParseException {
		Date date1 = getDate(a.getAs("tpep_pickup_datetime"));
		Calendar currentCalendar = Calendar.getInstance();
		currentCalendar.setTime(getDate(target));
		int month = currentCalendar.get(Calendar.MONTH);
		int year = currentCalendar.get(Calendar.YEAR);
		Calendar targetCalendar = Calendar.getInstance();
		targetCalendar.setTime(date1);
		int targetMonth = targetCalendar.get(Calendar.MONTH);
		int targetYear = targetCalendar.get(Calendar.YEAR);
		return month == targetMonth && year == targetYear;
	}
	public static boolean isRowDateInSpecificRange(Row a, String fromDate, String toDate) throws ParseException {
		Date date1 = getDate(a.getAs("tpep_pickup_datetime"));
		Date from = getDate(fromDate);
		Date to = getDate(toDate);
		Calendar targetCalendar = Calendar.getInstance();
		targetCalendar.setTime(date1);
		Calendar fromCalendar = Calendar.getInstance();
		fromCalendar.setTime(from);
		Calendar toCalendar = Calendar.getInstance();
		toCalendar.setTime(to);
		if(targetCalendar.after(fromCalendar) && targetCalendar.before(toCalendar)) {
			return true;
		}
		return false;
	}
	/******************/
	/**		Maps	 **/
	/******************/
//	//opt -> ==2 to count (put values to 1)
//	public static Tuple2 mapp(Row a,int opt) throws ParseException{
//		Integer key =0;
//		Integer value = 1;
//		Date toCheck = getDate(a.getAs("tpep_pickup_datetime"));
//		for(int i=0; i< Time_intervals.values().length; i++) {
//			Time_intervals t = Time_intervals.values()[i];
//			if(t.getStartTime().getHours() > t.getEndTime().getHours()) {
//				if(toCheck.getHours() >= t.getStartTime().getHours() || 
//						toCheck.getHours() < t.getEndTime().getHours()) {
//					key = i;
//					value = Integer.parseInt(a.getAs("passenger_count"));
//				}
//			}else {
//				if(toCheck.getHours() >= t.getStartTime().getHours() && 
//						toCheck.getHours() < t.getEndTime().getHours()) {
//					key = i;
//					value = Integer.parseInt(a.getAs("passenger_count"));
//				}
//			}
//		}
//		if(opt==2) {
//			value=1;
//		}
//		return new Tuple2(key,value);
//	}
	@SuppressWarnings("deprecation")
	public static Tuple2<Integer,Row> mappingEverything(Row a) throws ParseException{
		Integer key =0;
		Row value;
		Date toCheck = getDate(a.getAs("tpep_pickup_datetime"));
		for(int i=0; i< Time_intervals.values().length; i++) {
			Time_intervals t = Time_intervals.values()[i];
			if(t.getStartTime().getHours() > t.getEndTime().getHours()) {
				if(toCheck.getHours() >= t.getStartTime().getHours() || 
						toCheck.getHours() < t.getEndTime().getHours()) {
					key = i;
				}
			}else {
				if(toCheck.getHours() >= t.getStartTime().getHours() && 
						toCheck.getHours() < t.getEndTime().getHours()) {
					key = i;
				}
			}
		}
		value = RowFactory.create(
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("trip_distance")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("extra")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("tip_amount")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("total_amount")),1)),
				Float.parseFloat(a.getAs("trip_distance")),
				Float.parseFloat(a.getAs("tip_amount")),
				Float.parseFloat(a.getAs("total_amount")),
				1
				);
		return new Tuple2<Integer, Row>(key,value);
	}
	
	
	/*********************/
	/**		Reduces		**/
	/*********************/
	
	private static Row counts(Row a, Row b) {
		Row res = null;
//		Tuple2<Float,Integer> a0 = a.getAs(0);
//		Tuple2<Float,Integer> a1 = a.getAs(1);
//		Tuple2<Float,Integer> a2 = a.getAs(2);
//		Tuple2<Float,Integer> a3 = a.getAs(3);
//		Tuple2<Float,Integer> b0 = b.getAs(0);
//		Tuple2<Float,Integer> b1 = b.getAs(1);
//		Tuple2<Float,Integer> b2 = b.getAs(2);
//		Tuple2<Float,Integer> b3 = b.getAs(3);
//		Tuple2<Float,Integer> elem0 = a0._1>b0._1?a0:(a0._1<b0._1?b0:new Tuple2<Float,Integer>(a0._1,a0._2+b0._2));
//		Tuple2<Float,Integer> elem1 = a1._1>b1._1?a1:(a1._1<b1._1?b1:new Tuple2<Float,Integer>(a1._1,a1._2+b1._2));
//		Tuple2<Float,Integer> elem2 = a2._1>b2._1?a2:(a2._1<b2._1?b2:new Tuple2<Float,Integer>(a2._1,a2._2+b2._2));
//		Tuple2<Float,Integer> elem3 = a3._1>b3._1?a3:(a3._1<b3._1?b3:new Tuple2<Float,Integer>(a3._1,a3._2+b3._2));
		MaxValueManager a0 = a.getAs(0);
		MaxValueManager a1 = a.getAs(1);
		MaxValueManager a2 = a.getAs(2);
		MaxValueManager a3 = a.getAs(3);
		MaxValueManager b0 = b.getAs(0);
		MaxValueManager b1 = b.getAs(1);
		MaxValueManager b2 = b.getAs(2);
		MaxValueManager b3 = b.getAs(3);
		MaxValueManager elem0 = a0.setValues(b0);
		MaxValueManager elem1 = a1.setValues(b1);
		MaxValueManager elem2 = a2.setValues(b2);
		MaxValueManager elem3 = a3.setValues(b3);
		Float elem4 = (float)a.getAs(4) + (float)b.getAs(4);
		Float elem5 = (float)a.getAs(5) + (float)b.getAs(5);
		Float elem6 = (float)a.getAs(6) + (float)b.getAs(6);
		Integer elem7 = (int)a.getAs(7) + (int)b.getAs(7);
		res = RowFactory.create(elem0,
				elem1,
				elem2,
				elem3,
				elem4,
				elem5,
				elem6,
				elem7
				);
		
		return res;
	}
	
	
	/*********************/
	/**  Extra methods  **/
	/*********************/
	private static Date getDate(String dateInString) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return formatter.parse(dateInString);
	}
}
