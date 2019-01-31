package it.unitn.spark.project.custom_classes;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Objects;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class TaxyZone {

	private JavaRDD<Row> lookUpTable;
	private HashMap<String,Integer> boroughDistinct;
	private HashMap<Integer,String> borough;
	
	public TaxyZone(JavaRDD<Row> lookUpTable) {
		this.lookUpTable = lookUpTable;
		boroughDistinct = new HashMap<String, Integer>();
		borough = new HashMap<Integer,String>();
		JavaRDD<Object> temp = this.lookUpTable.map(a -> a.getAs("Borough")).distinct();
		Iterator<Object> it = temp.collect().iterator();
		int id=0;
		while(it.hasNext()) {
			Object o = it.next();
			String s = (String)o;
			boroughDistinct.put(s, id++);
		}
		JavaRDD<Tuple2<Integer, String>> temp2 = this.lookUpTable.map(a -> new Tuple2<Integer, String>(Integer.parseInt(a.getAs("LocationID")), a.getAs("Borough")));
		Iterator<Tuple2<Integer, String>> it2 = temp2.collect().iterator();
		while(it2.hasNext()) {
			Tuple2<Integer, String> x = it2.next();
			borough.put(x._1, x._2);
		}
	}
	
	public int getBoroughDistId(String boroughName) {
		return boroughDistinct.get(boroughName);
	}
	
	public String getBoroughDistName(int value) {
		return boroughDistinct.entrySet()
				.stream()
				.filter(entry -> Objects.equals(entry.getValue(), value))
				.findFirst()
				.get()
				.getKey();
	}
	
	public boolean sameBorough(Integer locIdA, Integer locIdB) {
		if(locIdA==locIdB) {
			return true;
		}
		String a = borough.get(locIdA);
		String b = borough.get(locIdB);
		if(a.equals(b)) {
			return true;
		}else {
			return false;
		}
	}
	public String getZone(Integer locId) {
		String s = "";
		s = lookUpTable.reduce((a,b) -> Integer.parseInt(a.getAs("LocationID"))==locId?a:b).getAs("Zone"); 
		return s;
	}
	
	public String getServiceZone(Integer locId) {
		String s = "";
		s = lookUpTable.reduce((a,b) -> Integer.parseInt(a.getAs("LocationID"))==locId?a:b).getAs("service_zone"); 
		return s;
	}
	
	public String getBorough(int locId) {
		String s = "";
		s = borough.get(locId); 
		return s;
	}
	
	public String boroughString() {
		String s = "[ ";
		for(String borName : this.boroughDistinct.keySet()) {
			s += borName +", ";
		}
		s += " ]";
		return s;
	}
}
