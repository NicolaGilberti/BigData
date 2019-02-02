package it.unitn.spark.project.custom_classes;

import java.util.ArrayList;
import java.io.Serializable;
import java.lang.Float;
import java.lang.Integer;
import scala.Tuple2;

public class MaxValueManager implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//attributes
	private ArrayList<Tuple2<Float,Integer>> maxValues;
	public int balanceTo = 3;
	//constructors
	public MaxValueManager(Tuple2<Float, Integer> f,Tuple2<Float, Integer> s,Tuple2<Float, Integer> t) {
		maxValues = new ArrayList<Tuple2<Float,Integer>>(3);
		maxValues.add(f);
		maxValues.add(s);
		maxValues.add(t);
	}
	public MaxValueManager(Tuple2<Float, Integer> f) {
		maxValues = new ArrayList<Tuple2<Float,Integer>>(3);
		maxValues.add(f);
		maxValues.add(new Tuple2<Float,Integer>(new Float(0),new Integer(1)));
		maxValues.add(new Tuple2<Float,Integer>(new Float(0),new Integer(1)));
	}
	//get & set
	public Tuple2<Float,Integer> getFirst() {
		return this.maxValues.get(0);
	}
	public void setFirst(Tuple2<Float,Integer> first) {
		maxValues.set(0, first);
	}
	public Tuple2<Float,Integer> getSecond() {
		return this.maxValues.get(1);
	}
	public void setSecond(Tuple2<Float,Integer> second) {
		maxValues.set(1, second);
	}
	public Tuple2<Float,Integer> getThird() {
		return this.maxValues.get(2);
	}
	public void setThird(Tuple2<Float,Integer> third) {
		maxValues.set(2, third);
	}
	//getValues
	public ArrayList<Tuple2<Float,Integer>> getValues() {
		return this.maxValues;
	}
	//setValues
	public MaxValueManager setValues(MaxValueManager b) {
		ArrayList<Tuple2<Float,Integer>> bList = b.getValues();
		for(int i=0; i< bList.size();i++) {
			Tuple2<Float,Integer> arr = bList.get(i);
			if (Float.compare(arr._1, this.getFirst()._1)>0){
				this.maxValues.add(0, arr);
            }else if (Float.compare(arr._1, this.getFirst()._1)==0){
            	this.setFirst(new Tuple2<Float, Integer>(this.getFirst()._1, Integer.sum(arr._2, this.getFirst()._2)));
            }else if (Float.compare(arr._1, this.getSecond()._1)>0){
				this.maxValues.add(1, arr);
            }else if (Float.compare(arr._1, this.getSecond()._1)==0){
            	this.setSecond(new Tuple2<Float, Integer>(this.getSecond()._1, Integer.sum(arr._2, this.getSecond()._2)));
            }else if (Float.compare(arr._1, this.getThird()._1)>0) {
				this.maxValues.add(2, arr);
            }else if (Float.compare(arr._1, this.getThird()._1)==0){
            	this.setThird(new Tuple2<Float, Integer>(this.getThird()._1, Integer.sum(arr._2, this.getThird()._2)));
            }
			this.balanceToX(balanceTo);
		}
		return this;
	}
	/**
	 * to maintain only X values
	 * inside the maxManager
	 * @param X
	 */
	private void balanceToX(int X) {
		if(this.maxValues.size()>X) {
			int elemToRem = this.maxValues.size();
			for(int j=X; j<elemToRem; j++) {
				this.maxValues.remove(X);
			}
		}
	}
	//toString
	@Override
	public String toString() {
		String s = "[value, #]=>";
			for(int i=0; i< this.maxValues.size(); i++) {
				Tuple2<Float, Integer> tmp = this.maxValues.get(i);
				s += "\t[" + tmp._1 + ", " + tmp._2 + "]";
			}
		return s;
	}
	
	public String toCSV() {
		String s="";
		for(int i=0; i< this.maxValues.size(); i++) {
			Tuple2<Float, Integer> tmp = this.maxValues.get(i);
			s += tmp._1 + "," + tmp._2 + ",";
		}
		return s;
	}
	
	public int getBalanceTo() {
		return balanceTo;
	}
	public void setBalanceTo(int balanceTo) {
		this.balanceTo = balanceTo;
	}
}
