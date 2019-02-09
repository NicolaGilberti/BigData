package it.unitn.spark.project.analysis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import it.unitn.spark.project.custom_classes.Time_intervals;
import it.unitn.spark.project.custom_classes.DayOfWeek;
import it.unitn.spark.project.custom_classes.MaxValueManager;
import it.unitn.spark.project.custom_classes.Payment_type;
import it.unitn.spark.project.custom_classes.TaxyZone;
import scala.Tuple2;

public class DateTimeAnalysis {
	private static Calendar calendar = Calendar.getInstance();
	private static TaxyZone taxyZone = null;
	public static int counterMan=0;
	/**
	 * Time intervals
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForTimeIntervals(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForTimeIntervals(a));
		return listWithKey;
	}
	/**
	 * Weekday-Weekend
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForWeekendWeekdays(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForWeekendWeekdays(a));
		return listWithKey;
	}
	/**
	 * Weekday-Weekend & Time intervals
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForWWTI(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForWWTI(a));
		return listWithKey;
	}
	/**
	 * Time-Intervals & Payment types
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForTIPT(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForTIPT(a));
		return listWithKey;
	}
	/**
	 * Weekday-Weekend & Payment types
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForWWPT(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForWWPT(a));
		return listWithKey;
	}
	/**
	 * Weekday-Weekend & Time-Intervals & Payment types
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForWWTIPT(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForWWTIPT(a));
		return listWithKey;
	}
	/**
	 * Weekday-Weekend & PU and DO in same/different borough
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForWWB(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForWWB(a));
		return listWithKey;
	}
	/**
	 * Weekday-Weekend & PU 
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForWWPU(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForWWPU(a));
		return listWithKey;
	}
	/**
	 * Weekday-Weekend & DO 
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForWWDO(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForWWDO(a));
		return listWithKey;
	}
	/**
	 * Weekday-Weekend & PU Borough
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForWWPUB(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForWWPUB(a));
		return listWithKey;
	}
	/**
	 * Weekday-Weekend & DO Borough 
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForWWDOB(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForWWDOB(a));
		return listWithKey;
	}
	/**
	 * Time-Intervals & PU and DO in same/different borough
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForTIB(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForTIB(a));
		return listWithKey;
	}
	/**
	 * Time-Intervals & PU 
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForTIPU(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForTIPU(a));
		return listWithKey;
	}
	/**
	 * Time-Intervals & DO 
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForTIDO(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForTIDO(a));
		return listWithKey;
	}
	/**
	 * Time-Intervals & PU Borough
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForTIPUB(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForTIPUB(a));
		return listWithKey;
	}
	/**
	 * Time-Intervals & DO Borough 
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer,Row> getValuableDataForTIDOB(JavaRDD<Row> fullList){
		JavaPairRDD<Integer,Row> listWithKey = null;
		listWithKey = fullList.mapToPair(a -> mapDataForTIDOB(a));
		return listWithKey;
	}
	/**
	 * 
	 * @param fullList
	 * @return
	 */
	public static JavaPairRDD<Integer, Row> getAllReducedData(JavaPairRDD<Integer,Row> fullList){
		JavaPairRDD<Integer,Row> listWithAvg = null;
		if(fullList.isEmpty()) {
			throw new java.lang.RuntimeException("Empty RDD");
		}
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
	public static boolean paymentType(Row a, Payment_type pt) {
		if(a.getAs("payment_type").equals(pt.getRealTag())) {
			return true;
		}
		return false;
	}
	/******************/
	/**		Maps	 **/
	/******************/
	public static Tuple2<Integer,Row> mapDataForTimeIntervals(Row a) throws ParseException{
		Integer key =0;
		Row value;
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(interval(a, Time_intervals.values()[i])) {
				key = i;
			}
		}
		value = getFormattedRow(a);
		return new Tuple2<Integer, Row>((key+1),value);
	}
	
	public static Tuple2<Integer,Row> mapDataForWeekendWeekdays(Row a) throws ParseException{
		int key = 0;
		Row value = null;
		if(weekEnd(a)) {
			key = DayOfWeek.WEEKEND.ordinal();
		}else {
			key = DayOfWeek.WEEKDAY.ordinal();
		}
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>((key+1),value);
	}
	
	public static Tuple2<Integer,Row> mapDataForWWTI(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		if(weekEnd(a)) {
			keyPt1 = DayOfWeek.WEEKEND.ordinal();
		}else {
			keyPt1 = DayOfWeek.WEEKDAY.ordinal();
		}
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(interval(a, Time_intervals.values()[i])) {
				keyPt2 = i;
			}
		}
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	public static Tuple2<Integer,Row> mapDataForTIPT(Row a) throws ParseException{
		boolean tmp = false;
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(interval(a, Time_intervals.values()[i])) {
				keyPt1 = i;
			}
		}
		/*for(int i=0; i< Payment_type.values().length; i++) {
			if(paymentType(a, Payment_type.values()[i])) {
				keyPt2 = i;
			}
		}*/
		keyPt2 = Integer.parseInt(a.getAs("payment_type"))-1;
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForWWPT(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		if(weekEnd(a)) {
			keyPt1 = DayOfWeek.WEEKEND.ordinal();
		}else {
			keyPt1 = DayOfWeek.WEEKDAY.ordinal();
		}
		/*for(int i=0; i< Payment_type.values().length; i++) {
			if(paymentType(a, Payment_type.values()[i])) {
				keyPt2 = i;
			}
		}*/
		keyPt2 = Integer.parseInt(a.getAs("payment_type"))-1;
		String keyS = (keyPt1+1) + "" + (keyPt2+1);
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForWWTIPT(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Integer keyPt3 = 0;
		Row value = null;
		if(weekEnd(a)) {
			keyPt1 = DayOfWeek.WEEKEND.ordinal();
		}else {
			keyPt1 = DayOfWeek.WEEKDAY.ordinal();
		}
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(interval(a, Time_intervals.values()[i])) {
				keyPt2 = i;
			}
		}
		/*for(int i=0; i< Payment_type.values().length; i++) {
			if(paymentType(a, Payment_type.values()[i])) {
				keyPt3 = i;
			}
		}*/
		keyPt3 = Integer.parseInt(a.getAs("payment_type"))-1;
		String keyS = (keyPt1+1) + "" + (keyPt2+1) + "" + (keyPt3+1);
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	
	public static Tuple2<Integer,Row> mapDataForWWB(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		if(weekEnd(a)) {
			keyPt1 = DayOfWeek.WEEKEND.ordinal();
		}else {
			keyPt1 = DayOfWeek.WEEKDAY.ordinal();
		}
		boolean keyPt2Bool = taxyZone.sameBorough(Integer.parseInt(a.getAs("PULocationID")),Integer.parseInt(a.getAs("DOLocationID")));
		keyPt2 = keyPt2Bool?1:2;
		String keyS = (keyPt1+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForWWPU(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		if(weekEnd(a)) {
			keyPt1 = DayOfWeek.WEEKEND.ordinal();
		}else {
			keyPt1 = DayOfWeek.WEEKDAY.ordinal();
		}
		keyPt2 = Integer.parseInt(a.getAs("PULocationID"));
		String keyS = (keyPt1+1) + "" + (FromTo.FROM.ordinal()+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForTIDO(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(interval(a, Time_intervals.values()[i])) {
				keyPt1 = i;
			}
		}
		keyPt2 = Integer.parseInt(a.getAs("DOLocationID"));
		String keyS = (keyPt1+1) + "" + (FromTo.TO.ordinal()+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForTIPUB(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(interval(a, Time_intervals.values()[i])) {
				keyPt1 = i;
			}
		}
		keyPt2 = taxyZone.getBoroughDistId(taxyZone.getBorough(Integer.parseInt(a.getAs("PULocationID"))));
		String keyS = (keyPt1+1) + "" + (FromTo.FROM.ordinal()+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForTIDOB(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(interval(a, Time_intervals.values()[i])) {
				keyPt1 = i;
			}
		}
		keyPt2 = taxyZone.getBoroughDistId(taxyZone.getBorough(Integer.parseInt(a.getAs("DOLocationID"))));
		String keyS = (keyPt1+1) + "" + (FromTo.TO.ordinal()+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForTIB(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(interval(a, Time_intervals.values()[i])) {
				keyPt1 = i;
			}
		}
		boolean keyPt2Bool = taxyZone.sameBorough(Integer.parseInt(a.getAs("PULocationID")),Integer.parseInt(a.getAs("DOLocationID")));
		keyPt2 = keyPt2Bool?1:2;
		String keyS = (keyPt1+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForTIPU(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		for(int i=0; i< Time_intervals.values().length; i++) {
			if(interval(a, Time_intervals.values()[i])) {
				keyPt1 = i;
			}
		}
		keyPt2 = Integer.parseInt(a.getAs("PULocationID"));
		String keyS = (keyPt1+1) + "" + (FromTo.FROM.ordinal()+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForWWDO(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		if(weekEnd(a)) {
			keyPt1 = DayOfWeek.WEEKEND.ordinal();
		}else {
			keyPt1 = DayOfWeek.WEEKDAY.ordinal();
		}
		keyPt2 = Integer.parseInt(a.getAs("DOLocationID"));
		String keyS = (keyPt1+1) + "" + (FromTo.TO.ordinal()+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForWWPUB(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		if(weekEnd(a)) {
			keyPt1 = DayOfWeek.WEEKEND.ordinal();
		}else {
			keyPt1 = DayOfWeek.WEEKDAY.ordinal();
		}
		keyPt2 = taxyZone.getBoroughDistId(taxyZone.getBorough(Integer.parseInt(a.getAs("PULocationID"))));
		String keyS = (keyPt1+1) + "" + (FromTo.FROM.ordinal()+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	public static Tuple2<Integer,Row> mapDataForWWDOB(Row a) throws ParseException{
		Integer keyPt1 = 0;
		Integer keyPt2 = 0;
		Row value = null;
		if(weekEnd(a)) {
			keyPt1 = DayOfWeek.WEEKEND.ordinal();
		}else {
			keyPt1 = DayOfWeek.WEEKDAY.ordinal();
		}
		keyPt2 = taxyZone.getBoroughDistId(taxyZone.getBorough(Integer.parseInt(a.getAs("DOLocationID"))));
		String keyS = (keyPt1+1) + "" + (FromTo.TO.ordinal()+1) + "" + keyPt2;
		int key = Integer.parseInt(keyS); 
		value = getFormattedRow(a);
		return new Tuple2<Integer,Row>(key,value);
	}
	/*********************/
	/**		Reduces		**/
	/*********************/
	
	private static Row counts(Row a, Row b) {
		Row res = null;
		MaxValueManager a0 = a.getAs(0);
		MaxValueManager a1 = a.getAs(1);
		MaxValueManager a2 = a.getAs(2);
		MaxValueManager a3 = a.getAs(3);
		MaxValueManager a4 = a.getAs(4);
		MaxValueManager b0 = b.getAs(0);
		MaxValueManager b1 = b.getAs(1);
		MaxValueManager b2 = b.getAs(2);
		MaxValueManager b3 = b.getAs(3);
		MaxValueManager b4 = b.getAs(4);
		MaxValueManager elem0 = a0.setValues(b0);
		MaxValueManager elem1 = a1.setValues(b1);
		MaxValueManager elem2 = a2.setValues(b2);
		MaxValueManager elem3 = a3.setValues(b3);
		MaxValueManager elem4 = a4.setValues(b4);
		Integer elem5 = (int)a.getAs(5) + (int)b.getAs(5);
		Float elem6 = (float)a.getAs(6) + (float)b.getAs(6);
		Float elem7 = (float)a.getAs(7) + (float)b.getAs(7);
		Float elem8 = (float)a.getAs(8) + (float)b.getAs(8);
		Integer elem9 = (int)a.getAs(9) + (int)b.getAs(9);
		res = RowFactory.create(elem0,
				elem1,
				elem2,
				elem3,
				elem4,
				elem5,
				elem6,
				elem7,
				elem8,
				elem9
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
	private static Row getFormattedRow(Row a){
		return RowFactory.create(
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("passenger_count")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("trip_distance")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("extra")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("tip_amount")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("total_amount")),1)),
				Integer.parseInt(a.getAs("passenger_count")),
				Float.parseFloat(a.getAs("trip_distance")),
				Float.parseFloat(a.getAs("tip_amount")),
				Float.parseFloat(a.getAs("total_amount")),
				1
				);
	}
	/**
	 * the Rows as to be in this form:
	 * 0- MaxValueManager maxPassengerCount
	 * 1- MaxValueManager maxTrip
	 * 2- MaxValueManager maxExtra
	 * 3- MaxValueManager maxTip
	 * 4- MaxValueManager maxTotal
	 * 5- Integer passengerCounter
	 * 6- Float sumTrip
	 * 7- Float sumTip
	 * 8- Float sumTotal
	 * 9- Integer counter 
	 * @param fullList the RDD to print
	 * @param keyComp to recover the correct key values
	 */
	public static void printDataAnalysis(JavaPairRDD<Integer,Row> fullList, Class<?> ... keyComp) {
		Iterator<Tuple2<Integer, Row>> it= fullList.collect().iterator();
		while(it.hasNext()) {
			String keyS = "";
			Tuple2<Integer, Row> tup = it.next();
			int key = tup._1;
			for(int i = keyComp.length-1; i >= 0; i--){
				Class<?> temp = keyComp[i];
				if(temp.isEnum()) {
					Object enumTempVal = temp.getEnumConstants()[key%10-1];
					keyS += temp.getSimpleName() + ": " + enumTempVal + " ";
				}else if(temp == Boolean.class){
					String tempVal = (key%10)==1?"the Same":"Different";
					keyS += "PU & DO are in " + tempVal + " borough ";
				}else if(temp == TaxyZone.class) {
					int tempVal = (key%10);
					key = key/10;
					Class<?> temp2 = keyComp[--i];
					Object tempVal2 = temp2.getEnumConstants()[key%10-1];
					keyS += tempVal2 + " " + taxyZone.getBoroughDistName(tempVal) + " ";
				}else if(temp == Integer.class) {
					int lengthN = (new Integer(key)).toString().length();
					int lengthO = keyComp.length;
					int tempVal;
					tempVal = (int) (key%Math.pow(10, (lengthN-lengthO)+1));
					key =  (key/(int)Math.pow(10, (lengthN-lengthO)+1));
					Class<?> temp2 = keyComp[--i];
					Object tempVal2 = temp2.getEnumConstants()[key%10-1];
					keyS += tempVal2 + " " + taxyZone.getZone(tempVal) + " ";
					
				}
				key = key/10;
			}
			keyS += "\nResults:\n";
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
	}
	/**
 * the Rows as to be in this form:
 * 0- MaxValueManager maxPassengerCount
 * 1- MaxValueManager maxTrip
 * 2- MaxValueManager maxExtra
 * 3- MaxValueManager maxTip
 * 4- MaxValueManager maxTotal
 * 5- Integer passengerCounter
 * 6- Float sumTrip
 * 7- Float sumTip
 * 8- Float sumTotal
 * 9- Integer counter 
 * @param fullList the RDD to print
 * @param keyComp to recover the correct key values
 */
public static String cSVDataAnalysis(JavaPairRDD<Integer,Row> fullList, Class<?> ... keyComp) {
	Iterator<Tuple2<Integer, Row>> it= fullList.collect().iterator();
	boolean header = true;
	String keyHeader = "";
	int balance;
	String csvRow = "";
	while(it.hasNext()) {
		String keyS = "";
		Tuple2<Integer, Row> tup = it.next();
		int key = tup._1;
		for(int i = keyComp.length-1; i >= 0; i--){
			Class<?> temp = keyComp[i];
			if(temp.isEnum()) {
				Object enumTempVal = temp.getEnumConstants()[key%10-1];
				keyS += temp.getSimpleName() + ",";
				csvRow += enumTempVal + ",";
			}else if(temp == Boolean.class){
				String tempVal = (key%10)==1?"Same":"Different";
				keyS += "PU/DO borough ";
				csvRow += tempVal + ",";
			}else if(temp == TaxyZone.class) {
				int tempVal = (key%10);
				key = key/10;
				Class<?> temp2 = keyComp[--i];
				Object tempVal2 = temp2.getEnumConstants()[key%10-1];
				keyS += tempVal2 + ",";
				csvRow += taxyZone.getBoroughDistName(tempVal) + ",";
			}else if(temp == Integer.class) {
				int lengthN = (new Integer(key)).toString().length();
				int lengthO = keyComp.length;
				int tempVal;
				tempVal = (int) (key%Math.pow(10, (lengthN-lengthO)+1));
				key =  (key/(int)Math.pow(10, (lengthN-lengthO)+1));
				Class<?> temp2 = keyComp[--i];
				Object tempVal2 = temp2.getEnumConstants()[key%10-1];
				keyS += tempVal2 + ",";
				csvRow += taxyZone.getZone(tempVal) + ",";
			}
			key = key/10;
		}
		MaxValueManager maxPassengerCount = tup._2.getAs(0);
		balance = maxPassengerCount.getBalanceTo();
		MaxValueManager maxTrip = tup._2.getAs(1);
		MaxValueManager maxExtra = tup._2.getAs(2);
		MaxValueManager maxTip = tup._2.getAs(3);
		MaxValueManager maxTotal = tup._2.getAs(4);
		Integer passengerCounter = tup._2.getAs(5);
		Float sumTrip = tup._2.getAs(6);
		Float sumTip = tup._2.getAs(7);
		Float sumTotal = tup._2.getAs(8);
		Integer counter = tup._2.getAs(9);
		csvRow += maxPassengerCount.toCSV();
		csvRow += maxTrip.toCSV();
		csvRow += maxExtra.toCSV();
		csvRow += maxTip.toCSV();
		csvRow += maxTotal.toCSV();
		csvRow += ((double)passengerCounter / counter*1.0) + ",";
		csvRow += ((double)sumTrip / counter*1.0) + ",";
		csvRow += ((double)sumTip / counter*1.0) + ",";
		csvRow += ((double)sumTotal / counter*1.0) + ",";
		csvRow += counter + "\n";
		if(header) {
			header = false;
			keyHeader = keyS;
			String[] var = new String[] {"MaxPassengerCount","MaxTripDistance","MaxExtraPaid","MaxTipPaid","MaxTotalPaid"};
			for(int j=0; j<var.length; j++) {
				for(int i=1; i<=balance; i++) {
					keyHeader += var[j] + "N" + i + ",freq" + var[j] + "N" + i + ",";
				}
			}
			keyHeader += "AvgPassengerCounter,AvgTripDistance,AvgTipPaid,AvgTotalPaid,RowEvaluated\n";
		}
	}
	return keyHeader+csvRow;
}
	public static void setTaxyZone(TaxyZone taxyZ) {
		taxyZone = taxyZ;
	}
	
	public enum FromTo{
		FROM(),TO();
	}
}

