package com.innovative.taxes.read;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.LogManager;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.util.Arrays;
import java.util.ArrayList;

public class BofaPdfStatementReader extends PdfReader{

	private static org.apache.log4j.Logger log = LogManager.getLogger(BofaPdfStatementReader.class);
	private boolean debit=false;
	transient int count=0;
	private String replaceReturn(String line)
	{
		String ret = "";//new String()
		char[] str = new char[line.length()];
		char[] chs =  line.toCharArray();
		for( int i=0;i<chs.length;i++ )
		{
			str[i]= (chs[i]=='\r') ?' ': chs[i];
	 	}
		 ret = new String(str);
		return ret;
	}
	private boolean checkNumber(String num)
	{
		boolean ret = true;
		for ( char ch:num.toCharArray())
		{
			if(!( (Character.isDigit(ch) ) || ch=='.' || ch =='-'  || ch =='\r'))
			{
				ret = false;
			}
		}
		return ret;
	}
	private String[] process(String[] lines) {
		String[] ret  = new String[lines.length];
		int start = 0;
		for(int i=0;i<lines.length;i++)
		{
			if(i>0 && lines[i].length() <30)
			{
				start = start ==0 ? i-1:start;
				start = StringUtils.isEmpty(ret[start])?i-2:start;
				ret[start]=ret[start].substring(0, ret[start].length()-1)+" "+lines[i];
				ret[start]=ret[start].replace("continued on the next page","").replace("\r", "");
			}
			else {
				start = start >0?0:start;
				ret[i]=lines[i].replace("\r", "");
			}
		}
		ret = removeNulls(ret);
		ret = removeExtraLines(ret);
		return ret;
	}
	private String[] removeExtraLines(String[] alllines)
	{
		int end =0;
		for(int i=0;i<alllines.length;i++)
		{
			if(alllines[i].startsWith("Total withdrawals")) {
				end = i;
				System.out.println("Breaking End is "+end);
				break;
			}
			alllines[i] = alllines[i].replace(",", "");
		}
		String subset[] =  Arrays.copyOfRange(alllines, 0, end + 1);
		return subset;
	}
	private String[] removeNulls(String[] alllines) 
		{
			List<String> nonnull= new ArrayList<>();
			for(int i=0;i<alllines.length;i++)
			{
				if(StringUtils.isEmpty(alllines[i])) continue;
				nonnull.add(alllines[i]);
			}
			return nonnull.toArray(new String[nonnull.size()]);
		}
	private int getEndIndex(String[] allLines, int start) {
		for (int i = start; i < allLines.length; i++) {
			if (allLines[i].contains("TOTAL INTEREST CHARGED")) {
				start = i;
			}
			else if (allLines[i].contains("Total withdrawals and other subtractions")) {
				start = allLines.length -1;
				break;
			}
		}
		return start;
	}
	private String startIndexStr="Total deposits and other additions ";
	
	public String getStartIndexStr() {
		return startIndexStr;
	}
	public void setStartIndexStr(String startIndexStr) {
		this.startIndexStr = startIndexStr;
	}
	private int getStartIndex(String[] allLines ) {
		int start = -1;
		int counter =count;
		for (int i = 0; i < allLines.length; i++) {
			if(debit) {
				if (allLines[i].contains(startIndexStr)) {
				
					counter--;
					if (counter <=0) {
						start = i+2;
						return start;
					}
				}	
			}
			
			if (allLines[i].contains("Purchases and Adjustments")) {
				start = i;
			}
		}
		return start;
	}

	private boolean ignore(String  line[])
	{
		String val = line[line.length-1];
		try {
			double vald= Double.parseDouble(val);
			return vald <=0;
		}catch(RuntimeException rne)
		{
			return false;
		}
	}
	private boolean checkDoc(String line) {
		return line.contains("Purchases and Adjustments $0.00");
	}
	public String[] getDocument(String text) {
		if(checkDoc(text))
			return null;
		String[] lines = text.split("\n");
		int start = getStartIndex(lines);
		if(start <0 || ignore(lines[start].replace("\r", "").split(" ")))
		{
			return null;
		}
		int end = getEndIndex(lines, start);
		 
		String subset[] =  Arrays.copyOfRange(lines, start, end + 1);
		if(debit) {
			subset = process(subset);
		}
 		return subset;
	}

	private File[] getFiles(String path, int card) {
		if (path.equals("BOFA")) {
			switch (card) {
			case 9275: {
				File dir = new File("D:\\Innovative Expenses\\2022filing\\bofa\\9275\\");
				return dir.listFiles();
			}
			case 4848: {
				File dir = new File("D:\\Innovative Expenses\\2022filing\\bofa\\4848\\");
				return dir.listFiles();
			}
			case 3045: {
				File dir = new File("D:\\Innovative Expenses\\2022filing\\bofa\\3045\\");
				return dir.listFiles();
			}
			case 5448: {
				File dir = new File("D:\\Innovative Expenses\\2022filing\\bofa\\5448\\");
				return dir.listFiles();
			}
			}
		}
		return null;
	}

	

	public void storeFiles() {
		try {
			storeFile(9275, "BOFA");
			storeFile(4848, "BOFA");
			storeFile(5448, "BOFA");
			storeFile(3045, "BOFA");
			System.out.println("Completed storing files");
		} catch (Exception esp) {
			esp.printStackTrace();
		}
	}
String out="D:\\Innovative Expenses\\2022filing\\bofa\\?.csv";
	public void storeFile(int card, String bank) throws IOException {
		File[] files = null;
		File fil = null;
		switch (card) {
		case 9275: {
			files = getFiles("BOFA", 9275);
			fil = new File(out.replace("?", "9275"));
			debit=false;
			fil.createNewFile();
			break;
		}
		case 4848: {
			files = getFiles("BOFA", 4848);
			fil = new File(out.replace("?", "4848"));//new File("D:\\Innovative Expenses\\2022filing\\bofa\\4848.csv");
			fil.createNewFile();
			debit=false;
			break;
		}
		case 3045: {
			files = getFiles("BOFA", 3045);
			fil = new File(out.replace("?", "3045"));//new File("D:\\Innovative Expenses\\2022filing\\bofa\\3045.csv");
			fil.createNewFile();
			startIndexStr="Withdrawals and other subtractions";
			count=2;
			debit=true;
			break;
		}
		case 5448: {
			files = getFiles("BOFA", 5448);
			fil = new File(out.replace("?", "5448"));//new File("D:\\Innovative Expenses\\2022filing\\bofa\\5448.csv");
			fil.createNewFile();
			count=2;
			startIndexStr="Deposits and other credits";
			debit=true;
			break;
		}
		}
		if (files != null && files.length > 0) {
			List<String> data = readCardData(files);
			System.out.println("Writing content to"+fil.getAbsolutePath());
			FileWriter fw = new FileWriter(fil);
			data.forEach(line -> {
				try {
					fw.write(line + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			fw.flush();
			fw.close();
		} else
			System.out.println("No files for card" + card);
	}

	public static void main(String ard[]) throws IOException {
		BasicConfigurator.configure(new ConsoleAppender());
		BofaPdfStatementReader reader = new BofaPdfStatementReader();
	 	reader.storeFiles();
	}
}
