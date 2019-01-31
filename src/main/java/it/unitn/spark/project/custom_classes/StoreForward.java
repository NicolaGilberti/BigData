package it.unitn.spark.project.custom_classes;

public enum StoreForward {
	STORE_FORWARD("Y"), NOT_STORE_FORWARD("N");
	
	private String status;

	StoreForward(String b) {
        this.status = b;
    }

    public String getStatus() {
        return this.status;
    }
}
