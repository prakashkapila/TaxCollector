package com.innovative.taxes.beans;

import java.io.Serializable;

public class GroupedColumn implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5583501243030373519L;
	String account;
	Double amount;
	String comment;
	public String getAccount() {
		return account;
	}
	public void setAccount(String account) {
		this.account = account;
	}
	public Double getAmount() {
		return amount;
	}
	public void setAmount(Double amount) {
		this.amount = amount;
	}
	
}
