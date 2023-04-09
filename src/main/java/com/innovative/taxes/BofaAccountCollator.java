package com.innovative.taxes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
import java.util.Arrays;

import com.innovative.taxes.beans.JPMDeductionsModel;
import com.innovative.taxes.beans.SparkSessionMgr;

public class BofaAccountCollator implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4163155203500847335L;
	private static Logger log = LogManager.getLogger(BofaAccountCollator.class);
	SparkSession sess = null;
	SparkSessionMgr mgr = new SparkSessionMgr();
	final String path1 = "D:\\Innovative Expenses\\2022filing\\summaries\\Result.csv";
	final String dir = "D:\\Innovative Expenses\\2022filing\\allCardsSummary\\bofa";
	final String[] bofaChkg=new String[]{"D:\\Innovative Expenses\\2022filing\\bofa\\3045.csv",
				"D:\\Innovative Expenses\\2022filing\\bofa\\4848.csv",
				"D:\\Innovative Expenses\\2022filing\\bofa\\5448.csv",
				"D:\\Innovative Expenses\\2022filing\\bofa\\9275.csv"
			};

	final String group="Grouped3.csv";

	public void groupAndSave()
	{
		sess=mgr.getSession();
		Dataset<Row> lines = sess.read().csv(bofaChkg);
		lines = lines.filter(new FilterFunction<Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public boolean call(Row value) throws Exception {
				return value!= null && StringUtils.isNotEmpty(value.mkString()) &&value.mkString().length()>2 && value.mkString().charAt(2)=='/';
			}
		});
		lines.show();
		MapFunction<Row,JPMDeductionsModel> mapper = new BofaModelMapper(); 
		Dataset<JPMDeductionsModel> model  = lines.map(mapper, Encoders.bean(JPMDeductionsModel.class));
	 	model.show();
		Dataset<Row> groupSum = model.groupBy(model.col("account")).sum("amount");
		groupSum.show();
//		model.select(new Column("amount").isNull()).show();
	
	//	Dataset<Row> groupSumDet = groupSum.join(model.select(new Column("desc"),new Column("account")),"account");
	//	groupSumDet.show(); 
	//	log.info("Total recs to save"+groupSumDet.count());
		saveGroup(groupSum, group);
	}
static 	FileWriter fw = null;
	 private void saveGroup(Dataset<Row> rows1,String filename) {
		 
		Dataset<Row> rows = rows1.coalesce(1);
		File f = new File(dir+filename);
		
		try {
			f.createNewFile();
			fw = new FileWriter(f,true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		rows.foreach(func->{
			String row = "";
			for (int i=0;i< func.length();i++)
			{
				row+=func.get(i)+(","); 
			}
			row+="\n";
			fw.write(row);
		});
		try {
			fw.flush();
			fw.close();
		} catch (IOException e) {
			
			e.printStackTrace();
			log.info(e.getMessage());
		}
 	}
	 public static void main(String arg[])
		{
			BofaAccountCollator acc = new BofaAccountCollator();
			log.getLogger("org").setLevel(Level.ERROR);
		//	log.getLogger("com.innovative").setLevel(Level.ALL);
			//log.getLogger("akka").setLevel(Level.OFF);
			//acc.collateAndSave();
			acc.groupAndSave();
		}
}

class BofaModelMapper implements MapFunction<Row,JPMDeductionsModel>{
	 
	private static final long serialVersionUID = 7140659695421277582L;
	private static Logger log = LogManager.getLogger(BofaModelMapper.class);
	JPMDeductionsModel mod;
	int acntIndx=-1;
	private boolean checkAccount(String acnt)
	{
		char[] chrs = acnt.toCharArray();
		for(int i=0;i<chrs.length;i++)
		{
			if(Character.isDigit(chrs[i])) {
				return false;
			}
		}
		return true;
	}
	private String[] combineNulls(String vals)
	{
		vals = vals.replace(",null","");
		vals =vals.replace("$", "").replace("[", "").replace("]", "").replace("\"","").replace("(EXCHG RATE)", "");
		return vals.replace(","," ").split(" ");
	}
	@Override
	public JPMDeductionsModel call(Row value) throws Exception {
		JPMDeductionsModel mod = new JPMDeductionsModel ();
		String vals[] = combineNulls(String.valueOf(value));
		if(vals.length <2 )
			return null;
		String amt = vals[vals.length-1];
		if(StringUtils.isEmpty(amt)||amt.contains("01/17")|| amt.contains("CO")||value.mkString().charAt(2)!='/')
				{
					log.info("Strop here"+value);
				}
		mod.setAmount(Double.valueOf(amt));
		mod.setTranDate(vals[0].replace("[", ""));
		String str= "";
		if(acntIndx==-1) { 
			acntIndx=checkAccount(vals[1])?1:2;
		}
			
		for(int i=1;i<vals.length;i++)
		{
			if(StringUtils.isNotBlank(vals[i])) {
				str += vals[i]+",";
				if(StringUtils.isEmpty(mod.getAccount()))
					mod.setAccount(vals[acntIndx]);
			}
		}
		mod.setDesc(str);
		return mod;
	}
}
