package com.innovative.taxes.read;

import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import com.innovative.taxes.beans.DeductionsModel;


public class RecordReader implements MapFunction<Row, DeductionsModel> {

	/**
	 * 
	 */
	private static Logger Log = LogManager.getLogger(RecordReader.class);
	private static final long serialVersionUID = 6253566602338077417L;
	static int card;
	final static int MASTER = 5302;
	final static int CHECK = 3045;
	final static int VISA_PER = 4848;
	final static int VISA_BUS = 5448;
	final static int MASTER1 = 2855;
	final static int VISACHASE=2662;
	final static int VISACHASE2=3908;
	
	String delimiter = "!@";
	
	public String processPaypal(String paypa)
	{
		String ret = "";
		// 01/06/20 PAYPAL           DES:INST XFER  ID:UBER  INDN:PRAKASH KAPILA          CO ID:PAYPALSI77 WEB -129.49 
		String words[] = paypa.split(" ");
		int i=0;
		if(paypa.contains("TRANSIT"))
		{ return "NJTRANSIT";}
		do {
			if(words[i].startsWith("ID:"))
				{
					ret=words[i].substring(words[i].indexOf(":")+1);
					break;
				}
			i++;
		}while (words[i] != null);
		Log.info("paypal account ="+ret);
		return ret;
	}
	public String getCsvRec(Row value) {
		String account = "", amt = "";
		String[] exemptions = new String[] { "Automatic Transfer from CHK 3045", "STEVEN D INTER B DES:DIRECT",
				"STEVEN D INTER B DES:DIRECT" };
		String ret = value.getString(0);
		String splits[] = ret.split(" ");
		if(splits.length <2)
			return null;
		amt = splits[splits.length - 1];
		
		switch (card) {
		case CHECK: {
			for (String ex : exemptions) {
				if (value.getString(0).contains(ex))
					return null;
			}
 			if(ret.contains("Nissan") || ret.contains("COMCAST"))
 			{
 				int STOP=100;
 				account = splits[1];
 			}
 			else
			if (ret.contains("OVERDRAFT PROTECTION FROM")) {
				account = "OVERDRAFT_PROTECTION";
				amt = "-" + amt;
			} else if (ret.contains("PAYPAL")) {
				account = processPaypal(ret);
			} else if (ret.contains("REMITLY")) {
				account = "REMITLY";
			} else {
				account = splits[1] + splits[2];
			}
			ret = account.trim() + delimiter + amt;
					
			return ret;
		}

		case MASTER: {
			account = splits[2] + splits[3];
			if(account.equalsIgnoreCase("EastWindsor") || account.equalsIgnoreCase("NEWJERSEY"))
			{
				account += splits[4];
			}
			return account + delimiter + amt;
		}
		case MASTER1: {
			amt = value.getString(0);
			splits = amt.split(" ");

			if (splits[splits.length - 1].startsWith("-"))
				return null;

			ret = splits[4] + splits[5] + delimiter+ splits[splits.length - 1];
			return ret;
		}
		case VISA_PER: {
			account = splits[2] + splits[3];
			return account + delimiter + amt;
		}
		case VISACHASE: {
			account = splits[2] + splits[3];
			return account + delimiter + amt;
		}
		case VISACHASE2: {
			account = splits[2] + splits[3];
			return account + delimiter + amt;
		}
		}
		return ret;
	}
	
	
	@Override
	public DeductionsModel call(Row value) throws Exception {
		DeductionsModel model = new DeductionsModel();
		String rec = this.getCsvRec(value);
		rec = rec== null ? "0"+delimiter+"0.00":rec;
		String acnAmt[] = rec.split(delimiter);
		if(acnAmt.length < 2)
		{
			Log.error(" Something is wrong ");
			Log.error(rec);
			model.setAccount("0");
			model.setAmount(0.00);
		}else {
		model.setAccount(acnAmt[0]);
		model.setAmount(acnAmt[1]);
		}
		return model;
	}

}
