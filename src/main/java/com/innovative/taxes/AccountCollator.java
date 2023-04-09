package com.innovative.taxes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

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

import com.innovative.taxes.beans.JPMDeductionsModel;
import com.innovative.taxes.beans.SparkSessionMgr;

public class AccountCollator implements Serializable {

	private static final long serialVersionUID = -1289593348808321212L;
	private static Logger log = LogManager.getLogger(AccountCollator.class);
	SparkSession sess = null;
	SparkSessionMgr mgr = new SparkSessionMgr();
	final String path1 = "D:\\Innovative Expenses\\2022filing\\Result4.csv";
	final String dir = "D:\\Innovative Expenses\\2022filing\\allCardsSummary\\";
	String[] bofaChkg = null;// = new String[] { "D:\\Innovative Expenses\\2022filing\\barclays\\2855.csv" };
	final String group = "Grouped1.csv";
	int acntIndx = 1;
	boolean start = false;

	private void init() {
		String[] files = new File(dir).list();
		bofaChkg = new String[files.length];
		for(int i=0;i<files.length;i++)
		{
			bofaChkg[i]=dir+files[i];
		}
 	}
	
	public void groupAndSave() {
		init();
		sess = mgr.getSession();
		Dataset<Row> lines = sess.read().csv(bofaChkg);
	 	lines.show();
		MapFunction<Row, JPMDeductionsModel> mapper = new AccountMapper();
		Dataset<JPMDeductionsModel> model = lines.map(mapper , Encoders.bean(JPMDeductionsModel.class));
		model.show();
		Dataset<Row> groupSum = model.groupBy("account").sum("amount");
		groupSum.show();
//		Dataset<Row> groupSumDet = groupSum.join(model.select(new Column("desc"), new Column("account")).distinct(),
//				"account");
 		log.info("Total recs to save" + groupSum.count());
		saveGroup(groupSum, group);
	}

	static FileWriter fw = null;

	private void saveGroup(Dataset<Row> rows1, String filename) {

		Dataset<Row> rows = rows1.dropDuplicates(new String[] { "account" }).coalesce(1);
		File f = new File(path1);

		try {
			f.createNewFile();
			fw = new FileWriter(f, true);
		} catch (IOException e) {
			e.printStackTrace();
		}

		rows.foreach(func -> {
			String row = "";
			for (int i = 0; i < func.length(); i++) {
				row += func.get(i) + (",");
			}
			row += "\n";
			fw.write(row);
		});
		try {
			fw.flush();
			fw.close();
		} catch (IOException e) {

			e.printStackTrace();
			log.info(e.getMessage());
		}
		log.info("saved File" + f.getAbsolutePath());
	}

	public static void main(String arg[]) {
		AccountCollator acc = new AccountCollator();
		log.getLogger("org").setLevel(Level.ERROR);
		log.getLogger("com.innovative").setLevel(Level.ALL);
		// log.getLogger("akka").setLevel(Level.OFF);
		// acc.collateAndSave();
		acc.groupAndSave();
	}
}

class AccountMapper implements MapFunction<Row, JPMDeductionsModel>{
	private static Logger log = LogManager.getLogger(AccountMapper.class);
	
	 int acntIndx=0; 
	 private static final long serialVersionUID = 1L;
		
		private boolean checkAccount(String acnt) {
			char[] chrs = acnt.toCharArray();
			for (int i = 0; i < chrs.length; i++) {
				if (!Character.isDigit(chrs[i])&& chrs[i]!='.'&& chrs[i]!='-') {
					return false;
				}
			}
			return true;
		}
		
		@Override
		public JPMDeductionsModel call(Row value) throws Exception {
			JPMDeductionsModel mod = new JPMDeductionsModel();
			String vals[] = String.valueOf(value).replace("$", "").replace("[", "").replace("]", "")
					.replace("\"", "").replace("(EXCHG RATE)", "").split(",");
			if (vals.length < 2 )
				return null;
			String amt = vals[acntIndx];
			if(!checkAccount(amt)){
				log.info(" Account value not set properly, checking with next value");
				if(checkAccount(vals[2]))
				{
					acntIndx=2;
					amt = vals[acntIndx];
				}else
				{
					if(checkAccount(vals[1]))
					{
						acntIndx=1;
						amt = vals[acntIndx];
					}
				}
			}
			mod.setAmount(Double.valueOf(amt));
			mod.setTranDate(vals[0].replace("[", ""));
	 		mod.setAccount(vals[0]);
			mod.setDesc(String.valueOf(value));
			return mod;
		}
		
	}
