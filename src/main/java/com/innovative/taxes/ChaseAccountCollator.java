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
		saveGroup(lines,group+"temp" );
		Dataset<JPMDeductionsModel> model  = lines.map(new MapFunction<Row,JPMDeductionsModel>(){
			private static final long serialVersionUID = 1L;
			private String removeSpaces(String space)
			{
				space=space.replace("$", "").replace("[", "").replace("]", "").replace("\"","");
				String[]spaces=space.split(" ");
				StringBuilder sb = new StringBuilder();
				for(int i=0;i<spaces.length;i++)
				{
					if(StringUtils.isEmpty(spaces[i].trim()))
						continue;
					sb.append(spaces[i]).append(" ");
				}
				
				return sb.toString();
			}
			@Override
			public JPMDeductionsModel call(Row value) throws Exception {
				JPMDeductionsModel mod = new JPMDeductionsModel ();
				String val = removeSpaces(String.valueOf(value));
				if(val.contains("EXCHG RATE"))
					return null;
				
				String vals[] = val.split(" ");
				if(val.contains("FOREIGN")) {
					vals[1]= vals[vals.length-4]+vals[vals.length-3];
				}
				if(vals.length <2 )
					return null;
				String amt = vals[vals.length-1];
				if(StringUtils.isEmpty(amt)||amt.contains("RUP"))
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
				if(StringUtils.isEmpty(mod.getAccount())||StringUtils.isEmpty(mod.getAmount().toString()))
					log.info("Stop here");
				return mod;
			}}, Encoders.bean(JPMDeductionsModel.class));
		saveGroup(model, out);
		
	}
	
	 private void saveGroup(Dataset<?> rows,String filename) {
		rows = rows.coalesce(1);
		File f = new File(dir+filename);
		
		try {
			f.createNewFile();
			fw = new FileWriter(f,true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		log.info("total rows to be saved are"+rows.count());
		rows.foreach(funcs->{
			String row = "";
			if(funcs instanceof Row)
			{
				Row func = (Row)funcs;
			for (int i=0;i< func.length();i++)
			{
				row+=func.get(i)+(","); 
			}
			}
			else if(funcs instanceof JPMDeductionsModel)
			{
				row = ((JPMDeductionsModel)funcs).getAccount()+","+((JPMDeductionsModel)funcs).getAmount();
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
