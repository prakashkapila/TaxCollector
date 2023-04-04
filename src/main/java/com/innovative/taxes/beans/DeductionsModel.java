package com.innovative.taxes.beans;

import java.io.Serializable;

public class DeductionsModel implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3386991618516728849L;
	String desc,date,location, category, account;
	Double amount;
	static String head = "date,account, category, amount";
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(date).append(",").append(account).append(",").append(category).append(",").append(amount).append("\n");
		return sb.toString();
	}
	public String getAccount() {
		return account;
	}
	public void setAccount(String account) {
		this.account = account;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public Double getAmount() {
		return amount;
	}

	public void setAmount(String amount) {
		if(amount == null)
			this.amount=0.0;
		if(amount.contains("Payment") || amount.contains("Amount") || amount.contains("INDN"))
		{
			int STOP = 100;
			amount = "0.00";
		}
		try {
		amount = amount.replace("$", "");	
		amount = amount.replace(",", "");
	 		this.amount = Double.valueOf(amount);
		this.amount = this.amount < 0 ? this.amount *-1:this.amount ;
		}catch(Exception esp)
		{
			esp.printStackTrace();
		}
	}
	public void setAmount(Double amount) {
		this.amount = amount;
	}
}
