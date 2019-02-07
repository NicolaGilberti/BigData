package it.unitn.spark.project.custom_classes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.Tuple2;

public class Helper {
	private static Calendar calendar = Calendar.getInstance();
	public static TaxyZone taxyZone = null;
	
	
	/*********************/
	/**		Filters		**/
	/*********************/
	public static boolean inInterval(Row a, Fare_Amount_Intervals d) throws ParseException {
		Float toCheck = Float.parseFloat(a.getAs("fare_amount"));
		if(toCheck >= d.getStartInterval() && 
				toCheck < d.getEndInterval()) {
			return true;
		}
		return false;
	}
	
	public static boolean inInterval(Row a, Distance_Intervals d) throws ParseException {
		Float toCheck = Float.parseFloat(a.getAs("trip_distance"));
		if(toCheck >= d.getStartInterval() && 
				toCheck < d.getEndInterval()) {
			return true;
		}
		return false;
	}
	
	@SuppressWarnings("deprecation")
	public static boolean inInterval(Row a, Time_intervals t) throws ParseException {
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
	
	
	private static Date getDate(String dateInString) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return formatter.parse(dateInString);
	}
	
	public static Boolean weekEnd(Row a) throws ParseException {
		Date dateA = getDate(a.getAs("tpep_pickup_datetime"));
		calendar.setTime(dateA);
		int toCheck = calendar.get(Calendar.DAY_OF_WEEK);
		if(toCheck == Calendar.SATURDAY ||
				toCheck == Calendar.SUNDAY) {
			return true;
		}
		return false;
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
	
	public static Row counts(Row a, Row b) {
		Row res = null;
		MaxValueManager a0 = a.getAs(0);
		MaxValueManager a1 = a.getAs(1);
		MaxValueManager a2 = a.getAs(2);
		MaxValueManager a3 = a.getAs(3);
		MaxValueManager a4 = a.getAs(4);
		MaxValueManager a5 = a.getAs(5);
		MaxValueManager a6 = a.getAs(6);
		MaxValueManager b0 = b.getAs(0);
		MaxValueManager b1 = b.getAs(1);
		MaxValueManager b2 = b.getAs(2);
		MaxValueManager b3 = b.getAs(3);
		MaxValueManager b4 = b.getAs(4);
		MaxValueManager b5 = b.getAs(5);
		MaxValueManager b6 = b.getAs(6);
		MaxValueManager elem0 = a0.setValues(b0);
		MaxValueManager elem1 = a1.setValues(b1);
		MaxValueManager elem2 = a2.setValues(b2);
		MaxValueManager elem3 = a3.setValues(b3);
		MaxValueManager elem4 = a4.setValues(b4);
		MaxValueManager elem5 = a5.setValues(b5);
		MaxValueManager elem6 = a6.setValues(b6);
		Integer elem7 = (int)a.getAs(7) + (int)b.getAs(7);
		Float elem8 = (float)a.getAs(8) + (float)b.getAs(8);
		Float elem9 = (float)a.getAs(9) + (float)b.getAs(9);
		Float elem10 = (float)a.getAs(10) + (float)b.getAs(10);
		Float elem11 = (float)a.getAs(11) + (float)b.getAs(11);
		Float elem12 = (float)a.getAs(12) + (float)b.getAs(12);
		Integer elem13 = (int)a.getAs(13) + (int)b.getAs(13);
		res = RowFactory.create(elem0,
				elem1,
				elem2,
				elem3,
				elem4,
				elem5,
				elem6,
				elem7,
				elem8,
				elem9,
				elem10,
				elem11,
				elem12,
				elem13
				);
		
		return res;
	}

	
	/*********************/
	/**  Extra methods  **/
	/*********************/
	/* @throws ParseException 
	 * @throws NumberFormatException */
	
	public static Row getFormattedRow(Row a) throws NumberFormatException, ParseException{
		return RowFactory.create(
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("passenger_count")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("trip_distance")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("extra")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("tip_amount")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("total_amount")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("tolls_amount")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(getTimeDiff(a),1)),
				Integer.parseInt(a.getAs("passenger_count")),
				Float.parseFloat(a.getAs("trip_distance")),
				Float.parseFloat(a.getAs("tip_amount")),
				Float.parseFloat(a.getAs("total_amount")),
				Float.parseFloat(a.getAs("tolls_amount")),
				getTimeDiff(a),
				1
				);
	}

		private static Float getTimeDiff(Row a) throws ParseException {
			Date puTime = getDate(a.getAs("tpep_pickup_datetime"));
			Date doTime = getDate(a.getAs("tpep_dropoff_datetime"));
			long diff = doTime.getTime()-puTime.getTime();
		return (float) ((diff)/ (60 * 1000));
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
	 * @param fullList
	 * @param keyComp
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
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
					if(i>0) {
						Class<?> temp2 = keyComp[--i];
						key = key/10;
						if(temp2.isEnum()) {
							Object tempVal2 = temp2.getEnumConstants()[key%10-1];
							keyS += temp2.getSimpleName() + ": " +tempVal2 + " " + taxyZone.getBorough(tempVal) + " ";
						}
						else if(temp2 == TaxyZone.class) {
							keyS +="FROM: "+ taxyZone.getBoroughDistName(key%10); 
							keyS +=", TO: " + taxyZone.getBoroughDistName(tempVal) + " ";
						}
					}
				}else if(temp == Integer.class) {
					//TODO: tempVal can be more than 1 character....
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
			MaxValueManager maxToll = tup._2.getAs(5);
			MaxValueManager maxTime = tup._2.getAs(6);
			Integer passengerCounter = tup._2.getAs(7);
			Float sumTrip = tup._2.getAs(8);
			Float sumTip = tup._2.getAs(9);
			Float sumTotal = tup._2.getAs(10);
			Float sumToll = tup._2.getAs(11);
			Float sumTime = tup._2.getAs(12);
			Integer counter = tup._2.getAs(13);
			formattedRow += "\tMaxPassengerCount: " + maxPassengerCount + "\n";
			formattedRow += "\tMaxTripDistance: " + maxTrip + "\n";
			formattedRow += "\tMaxExtraPaid: " + maxExtra + "\n";
			formattedRow += "\tMaxTipPaid: " + maxTip + "\n";
			formattedRow += "\tMaxTotalPaid: " + maxTotal + "\n";
			formattedRow += "\tMaxTollPaid: " + maxToll + "\n";
			formattedRow += "\tMaxTimeOfRide: " + maxTime + "\n";
			formattedRow += "\tAvgPassengerCounter: " + ((double)passengerCounter / counter*1.0) + "\n";
			formattedRow += "\tAvgTripDistance: " + ((double)sumTrip / counter*1.0) + "\n";
			formattedRow += "\tAvgTipPaid: " + ((double)sumTip / counter*1.0) + "\n";
			formattedRow += "\tAvgTotalPaid: " + ((double)sumTotal / counter*1.0) + "\n";
			formattedRow += "\tAvgTollPaid: " + ((double)sumToll / counter*1.0) + "\n";
			formattedRow += "\tAvgTimeOfRide: " + ((double)sumTime / counter*1.0) + "\n";
			formattedRow += "\tRowEvaluated: " + counter + "\n";
			System.out.println(keyS + formattedRow);
		}
	}
	
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
					if(i>0) {
						Class<?> temp2 = keyComp[--i];
						key = key/10;
						if(temp2.isEnum()) {
							Object tempVal2 = temp2.getEnumConstants()[key%10-1];
							keyS += tempVal2 + ",";
							csvRow += taxyZone.getBorough(tempVal) + ",";
						}
						else if(temp2 == TaxyZone.class) {
							keyS += "PU_borough,";
							csvRow += taxyZone.getBoroughDistName(key%10) + ",";
							keyS += "DO_borough,";
							csvRow += taxyZone.getBoroughDistName(tempVal) + ",";
						}
					}
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
			MaxValueManager maxToll = tup._2.getAs(5);
			MaxValueManager maxTime = tup._2.getAs(6);
			Integer passengerCounter = tup._2.getAs(7);
			Float sumTrip = tup._2.getAs(8);
			Float sumTip = tup._2.getAs(9);
			Float sumTotal = tup._2.getAs(10);
			Float sumToll = tup._2.getAs(11);
			Float sumTime = tup._2.getAs(12);
			Integer counter = tup._2.getAs(13);
			csvRow += maxPassengerCount.toCSV();
			csvRow += maxTrip.toCSV();
			csvRow += maxExtra.toCSV();
			csvRow += maxTip.toCSV();
			csvRow += maxTotal.toCSV();
			csvRow += maxToll.toCSV();
			csvRow += maxTime.toCSV();
			csvRow += ((double)passengerCounter / counter*1.0) + ",";
			csvRow += ((double)sumTrip / counter*1.0) + ",";
			csvRow += ((double)sumTip / counter*1.0) + ",";
			csvRow += ((double)sumTotal / counter*1.0) + ",";
			csvRow += ((double)sumToll / counter*1.0) + ",";
			csvRow += ((double)sumTime / counter*1.0) + ",";
			csvRow += counter + "\n";
			if(header) {
				header = false;
				keyHeader = keyS;
				String[] var = new String[] {"MaxPassengerCount","MaxTripDistance","MaxExtraPaid","MaxTipPaid","MaxTotalPaid","MaxTollPaid","MaxTime"};
				for(int j=0; j<var.length; j++) {
					for(int i=1; i<=balance; i++) {
						keyHeader += var[j] + "N" + i + ",freq" + var[j] + "N" + i + ",";
					}
				}
				keyHeader += "AvgPassengerCounter,AvgTripDistance,AvgTipPaid,AvgTotalPaid,AvgTollPaid,AvgTime,RowEvaluated\n";
			}
		}
		return keyHeader+csvRow;
	}
	
	public static void setTaxyZone(TaxyZone taxyZ) {
		taxyZone = taxyZ;
	}

}
