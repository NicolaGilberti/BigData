package it.unitn.spark.project.custom_classes;

import java.util.Date;

public enum Time_intervals {
	MORNING(4, 12), AFTERNOON(12, 20), NIGHT(20, 4);

    private Date startTime;
    private Date endTime;

    @SuppressWarnings("deprecation")
	Time_intervals(int s, int e) {
        this.startTime = new Date(0,0,0,s,0);
        this.endTime = new Date(0,0,0,e,0);
    }

    public Date getStartTime() {
        return this.startTime;
    }
    public Date getEndTime() {
        return this.endTime;
    }
}
