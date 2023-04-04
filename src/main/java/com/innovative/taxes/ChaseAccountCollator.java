package com.innovative.taxes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

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
import org.apache.spark.sql.functions;

import com.innovative.taxes.beans.DeductionsModel;
import com.innovative.taxes.beans.GroupedColumn;
import com.innovative.taxes.beans.JPMDeductionsModel;

import com.innovative.taxes.beans.GroupedColumnMapping;
import com.innovative.taxes.beans.SparkSessionMgr;
import com.innovative.taxes.read.RecordReader;


public class ChaseAccountCollator implements Serializable{
	private static final long serialVersionUID = -3672338536573560197L;
	private static Logger log = LogManager.getLogger(ChaseAccountCollator.class);
	SparkSessionMgr mgr = new SparkSessionMgr();
	final String path1 = "D:\\Innovative Expenses\\2022filing\\summaries\\Result.csv";
	final String dir = "D:\\Innovative Expenses\\2022filing\\allCardsSummary\\chase";
	final String[] chasefiles=new String[]{"D:\\Innovative Expenses\\2022filing\\chase\\2662.csv","D:\\Innovative Expenses\\2022filing\\chase\\3908.csv"};
	String ext[] = new String[]{"3045_Wrong.txt"};
	//2855_changes.txt",
 	String path[]=new String[]{"3045_changes.txt","2662_changes.txt","5302_changes.txt",
			"3908_changes.txt","4848_changes.txt"};
	final String out = "UnGrouped.csv";
	final String group="Grouped1.csv";
 	static FileWriter fw;
	Dataset<Row> allLines = null;
	SparkSession sess = null;
	
	public void collateAndSave() {
		Dataset<Row> allSet = null;
		Dataset<DeductionsModel> model = null;
		for(String fil:path)
		{
			int type = Integer.valueOf(fil.split("_")[0]);
		//	RecordReader.card=type;
		 	Dataset<Row> lines= sess.read().text(dir+fil);//.as(Encoders.bean(DeductionsModel.class));
			model = lines.map(new RecordReader(), Encoders.bean(DeductionsModel.class));
			log.info(" grouping for file"+fil);
			allSet = allSet== null ? model.select("account","amount"):
				allSet.select("account","amount").union(model.select("account","amount"));
			saveGroup(model.select("account","amount"),out);
			log.info("total rows in Allset"+allSet.count());
			log.info("total rows in model "+model.count()+" for file "+fil);
			allSet.show();
			allSet.cache();
		}
		
 	  }
	public void groupAndSave()
	{
		sess=mgr.getSession();
		Dataset<Row> lines = sess.read().csv(chasefiles);
		String metaDta ="Transaction Merchant  Name or Transaction Description $ Amount";
		lines = lines.filter(new FilterFunction<Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public boolean call(Row value) throws Exception {
				return value!= null && StringUtils.isNotEmpty(value.mkString()) &&value.mkString().length()>2 && value.mkString().charAt(2)=='/';
			}
		});
		lines.show();
		Dataset<JPMDeductionsModel> model  = lines.map(new MapFunction<Row,JPMDeductionsModel>(){
			private static final long serialVersionUID = 1L;
			@Override
			public JPMDeductionsModel call(Row value) throws Exception {
				JPMDeductionsModel mod = new JPMDeductionsModel ();
				String vals[] = String.valueOf(value).replace("$", "").replace("[", "").replace("]", "").replace("\"","").replace("(EXCHG RATE)", "").split(" ");
				if(vals.length <2 )
					return null;
				String amt = vals[vals.length-1];
				if(StringUtils.isEmpty(amt))
						{
					log.info("Strop here"+value);
						}
				mod.setAmount(Double.valueOf(amt));
				mod.setTranDate(vals[0].replace("[", ""));
				String str= "";
				for(int i=1;i<vals.length;i++)
				{
					if(StringUtils.isNotBlank(vals[i])) {
						str += vals[i]+",";
						if(StringUtils.isEmpty(mod.getAccount()))
							mod.setAccount(vals[i]);
					}
				}
				mod.setDesc(str);
				return mod;
			}}, Encoders.bean(JPMDeductionsModel.class));
	
		model.show();
		model.filter(new Column("amount").isNull()).show();
		Dataset<Row> groupSum = model.filter(new Column("amount").isNotNull()).groupBy("account").sum("amount");
		groupSum.show();
		Dataset<Row> groupSumDet = groupSum.join(model.select(new Column("desc"),new Column("account")),"account");
		groupSumDet.show(); 
		log.info("Total recs to save"+groupSumDet.count());
		saveGroup(groupSum, group);
	}
	 private void saveGroup(Dataset<Row> rows,String filename) {
		rows = rows.coalesce(1);
		File f = new File(dir+filename);
		
		try {
			f.createNewFile();
			fw = new FileWriter(f,true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		log.info("total rows to be saved are"+rows.count());
		rows.foreach(func->{
			String row = "";
			for (int i=0;i< func.length();i++)
			{
				row+=func.get(i)+(","); 
			}
			row+="\n";
			fw.write(row);
			fw.flush();
 		});
		try {
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
 	}
	
	
	public static void main(String arg[])
	{
		ChaseAccountCollator acc = new ChaseAccountCollator();
		log.getLogger("org").setLevel(Level.ERROR);
		log.getLogger("com.innovative").setLevel(Level.ALL);
		acc.groupAndSave();
	}
}
