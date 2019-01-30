package it.unitn.spark.project.custom_classes;

public enum Distance_Intervals {
	VERY_SHORT(0, 2), SHORT(2, 5), MEDIUM(5, 20), LONG(20, 200), VERY_LONG(200, 100000000);
	
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
