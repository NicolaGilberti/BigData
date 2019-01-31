package it.unitn.spark.project.custom_classes;

import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.Tuple2;

public class Helper {

	public static TaxyZone taxyZone = null;
	
	/*********************/
	/**  Extra methods  **/
	/*********************/
	
	public static Row getFormattedRow(Row a){
		return RowFactory.create(
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("passenger_count")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("trip_distance")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("extra")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("tip_amount")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("total_amount")),1)),
				new MaxValueManager(new Tuple2<Float,Integer>(Float.parseFloat(a.getAs("tolls_amount")),1)),
				Integer.parseInt(a.getAs("passenger_count")),
				Float.parseFloat(a.getAs("trip_distance")),
				Float.parseFloat(a.getAs("tip_amount")),
				Float.parseFloat(a.getAs("total_amount")),
				Float.parseFloat(a.getAs("tolls_amount")),
				1
				);
	}
	
	public static Row counts(Row a, Row b) {
		Row res = null;
		MaxValueManager a0 = a.getAs(0);
		MaxValueManager a1 = a.getAs(1);
		MaxValueManager a2 = a.getAs(2);
		MaxValueManager a3 = a.getAs(3);
		MaxValueManager a4 = a.getAs(4);
		MaxValueManager a5 = a.getAs(5);
		MaxValueManager b0 = b.getAs(0);
		MaxValueManager b1 = b.getAs(1);
		MaxValueManager b2 = b.getAs(2);
		MaxValueManager b3 = b.getAs(3);
		MaxValueManager b4 = b.getAs(4);
		MaxValueManager b5 = b.getAs(5);
		MaxValueManager elem0 = a0.setValues(b0);
		MaxValueManager elem1 = a1.setValues(b1);
		MaxValueManager elem2 = a2.setValues(b2);
		MaxValueManager elem3 = a3.setValues(b3);
		MaxValueManager elem4 = a4.setValues(b4);
		MaxValueManager elem5 = a5.setValues(b5);
		Integer elem6 = (int)a.getAs(6) + (int)b.getAs(6);
		Float elem7 = (float)a.getAs(7) + (float)b.getAs(7);
		Float elem8 = (float)a.getAs(8) + (float)b.getAs(8);
		Float elem9 = (float)a.getAs(9) + (float)b.getAs(9);
		Float elem10 = (float)a.getAs(10) + (float)b.getAs(10);
		Integer elem11 = (int)a.getAs(11) + (int)b.getAs(11);
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
				elem11
				);
		
		return res;
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
							keyS += tempVal2 + " " + taxyZone.getBorough(tempVal) + " ";
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
			Integer passengerCounter = tup._2.getAs(6);
			Float sumTrip = tup._2.getAs(7);
			Float sumTip = tup._2.getAs(8);
			Float sumTotal = tup._2.getAs(9);
			Float sumToll = tup._2.getAs(10);
			Integer counter = tup._2.getAs(11);
			formattedRow += "\tMaxPassengerCount: " + maxPassengerCount + "\n";
			formattedRow += "\tMaxTripDistance: " + maxTrip + "\n";
			formattedRow += "\tMaxExtraPaid: " + maxExtra + "\n";
			formattedRow += "\tMaxTipPaid: " + maxTip + "\n";
			formattedRow += "\tMaxTotalPaid: " + maxTotal + "\n";
			formattedRow += "\tMaxTollPaid: " + maxToll + "\n";
			formattedRow += "\tAvgPassengerCounter: " + ((double)passengerCounter / counter*1.0) + "\n";
			formattedRow += "\tAvgTripDistance: " + ((double)sumTrip / counter*1.0) + "\n";
			formattedRow += "\tAvgTipPaid: " + ((double)sumTip / counter*1.0) + "\n";
			formattedRow += "\tAvgTotalPaid: " + ((double)sumTotal / counter*1.0) + "\n";
			formattedRow += "\tAvgTollPaid: " + ((double)sumToll / counter*1.0) + "\n";
			formattedRow += "\tRowEvaluated: " + counter + "\n";
			System.out.println(keyS + formattedRow);
		}
	}
	public static void setTaxyZone(TaxyZone taxyZ) {
		taxyZone = taxyZ;
	}

}
