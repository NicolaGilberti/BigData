package it.unitn.spark.project.custom_classes;

public enum Payment_type {
	CREDIT_CARD(1), CASH(2), NO_CHARGE(3), DISPUTE(4), UNKNOWN(5), VOIDED_TRIP(6);
	int realTag;
	
	Payment_type(int realTag) {
		this.realTag = realTag;
	}
	
	public int getRealTag() {
		return this.realTag;
	}
}
