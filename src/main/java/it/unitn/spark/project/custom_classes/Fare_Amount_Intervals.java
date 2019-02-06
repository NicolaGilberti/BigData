package it.unitn.spark.project.custom_classes;

public enum Fare_Amount_Intervals {
	ZERO_TWO(0, 2), TWO_FIVE(2, 5), FIVE_TEN(5, 10), TEN_FIFTEEN(10, 15), FIFTEEN_TWENTYFIVE(15, 25), TWENTYFIVE_FOURTY(25, 40), FOURTY_HUNDRED(40, 100), HUNDRED_THREEHUNDRED(100, 300), THREEHUNDRED_THOUSAND(300, 1000);
	
	private Integer startInterval;
    private Integer endInterval;

    Fare_Amount_Intervals(int s, int e) {
        this.startInterval = s;
        this.endInterval = e;
    }

    public Integer getStartInterval() {
        return this.startInterval;
    }
    public Integer getEndInterval() {
        return this.endInterval;
    }

}
