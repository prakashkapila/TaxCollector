package com.innovative.taxes.beans;


import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class GroupedColumnMapping implements MapFunction<Row, GroupedColumn> {
	private static final long serialVersionUID = 1299691826395789436L;
	private static Logger log = LogManager.getLogger(GroupedColumnMapping.class);
	@Override
	public GroupedColumn call(Row value) throws Exception {
		GroupedColumn gc = new GroupedColumn();
		if(value==null)
			return null;
		try {
 		String[] acnt = value.getString(0).split(",");
		gc.setAccount(acnt[0]);
		gc.setAmount(Double.valueOf(acnt[1]));
		}catch(Exception esp) {
			
			log.info(value);
		}
		return gc;
	}
	private boolean check(String val)
	{
		char[] allch= val.toCharArray();
		return false;
	}
}
