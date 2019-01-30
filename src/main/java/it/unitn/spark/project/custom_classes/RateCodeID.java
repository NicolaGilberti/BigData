package it.unitn.spark.project.custom_classes;

public enum RateCodeID {
	STANDARD_RATE(1), JFK(2), NEWARK(3), NASSAU_OR_WESTCHESTER(4), NEGOTIATED_FARE(5), GROUP_RIDE(6), FALSE(99);
	int realTag;
	
	RateCodeID(int realTag) {
		this.realTag = realTag;
	}
	
	public int getRealTag() {
		return this.realTag;
	}

}
