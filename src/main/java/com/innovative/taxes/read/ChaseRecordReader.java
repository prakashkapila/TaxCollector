package com.innovative.taxes.read;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.innovative.taxes.beans.DeductionsModel;

public class ChaseRecordReader implements MapFunction<Row, DeductionsModel> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6253566602338077418L;
	static int card;
	final static int MASTER = 5302;
	final static int CHECK = 3045;
	final static int VISA_PER = 4848;
	final static int VISA_BUS = 5448;
	public String getCsvRec(Row value) {
		String account = "", amt = "";
		String[] exemptions = new String[] { "Automatic Transfer from CHK 3045", "STEVEN D INTER B DES:DIRECT",
				"STEVEN D INTER B DES:DIRECT" };
		String[] splits = value.getString(0).split(" ");
		 amt = splits[splits.length-1];
		 account = splits[1]+splits[2];
	 	 return account+","+amt;
	}
	
	@Override
	public DeductionsModel call(Row value) throws Exception {
		// TODO Auto-generated method stub
		DeductionsModel ret = new DeductionsModel ();
		String rec = getCsvRec(value);
		String recs[] =rec != null ? rec.split(","):new String[]{"0,0"}; 
		ret.setAccount(recs[0]);
		ret.setAmount(recs[1]);
		return null;
	}


}
