package it.unitn.spark.project.custom_classes;

public enum VenderID {
	ONE(1), TWO(2), THREE(3), FOUR(4);
	int realTag;
	
	VenderID(int realTag) {
		this.realTag = realTag;
	}
	
	public int getRealTag() {
		return this.realTag;
	}

}
