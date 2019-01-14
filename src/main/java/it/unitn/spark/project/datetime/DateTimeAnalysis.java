package it.unitn.spark.project.datetime;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import it.unitn.spark.project.custom_classes.Time_intervals;
import scala.Tuple2;

public class DateTimeAnalysis {
	private static Calendar calendar = Calendar.getInstance();
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
	/******************/
	/**		Maps	 **/
	/******************/
	
	
	
	/*********************/
	/**		Reduces		**/
	/*********************/
	
	
	
	/*********************/
	/**  Extra methods  **/
	/*********************/
	private static Date getDate(String dateInString) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return formatter.parse(dateInString);
	}
}
