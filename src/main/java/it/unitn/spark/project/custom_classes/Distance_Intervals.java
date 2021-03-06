package it.unitn.spark.project.custom_classes;

public enum Distance_Intervals {
	ZERO_TWO(0, 2), TWO_FIVE(2, 5), FIVE_TWENTY(5, 20), TWENTY_TWOHUNDRED(20, 200), TWOHUNDRED_PLUS(200, 100000000);
	
	private Integer startInterval;
    private Integer endInterval;

    Distance_Intervals(int s, int e) {
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
