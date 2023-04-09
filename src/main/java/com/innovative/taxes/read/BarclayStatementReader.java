package com.innovative.taxes.read;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;

public class BarclayStatementReader extends PdfReader {
	private static Logger log = LogManager.getLogger(BarclayStatementReader.class);
	String dir = "D:\\Innovative Expenses\\2022filing\\barclays\\2855\\";
	boolean segment= false;
	
	public String[] getDocument(String text) {
		String[] lines = text.split("\n");
		int start = getStartIndex(lines);
		int end = getEndIndex(lines, start);
		String subset[] =  Arrays.copyOfRange(lines, start, end );
		while(segment)
		{
			String[] newLines = Arrays.copyOfRange(lines, end, lines.length-1);
			String[] another = null;
			start = getStartIndex(newLines);
			segment = false;
			end = getEndIndex(newLines, start);
			another = Arrays.copyOfRange(newLines, start+1, end );
			List<String> ret =new ArrayList<>();
			Arrays.asList(subset).stream().forEach(cns->ret.add(cns));
			Arrays.asList(another).stream().forEach(cns->ret.add(cns));
			subset = ret.toArray(new String[0]);
		}
		subset = process(subset);
 		return subset;
	}
	
	private String[] process(String[] lines) {
		String[] ret  = new String[lines.length];
		int start = 0;
		for(int i=4;i<lines.length;i++)
		{
			if(i>0 && lines[i].length() <30)
			{
				start = start ==0 ? i-2:start;
				start = StringUtils.isEmpty(ret[start])?i-2:start;
				ret[start]=ret[start].substring(0, ret[i-2].length()-1)+" "+lines[start+1]+lines[start+2];
				ret[start]=ret[start].replace("continued on the next page","").replace("\r", "");
				ret[start+1]=null;
			}
			else {
				start = start >0?0:start;
				ret[i]=lines[i].replace("\r", "");
				if(lines[i].contains("Total purchase activity"))
				{
					break;
				}
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
			if(alllines[i].startsWith("Total interest for this period")) {
				end = i;
				System.out.println("Breaking End is "+end);
				break;
			}
			alllines[i] = alllines[i].replace(",", "");
		}
		end = end == 0?alllines.length:end;
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
	 		segment  = allLines[i].contains("continued on page");
 			if (allLines[i].startsWith("This Year-to-date") || segment ) {
				start = i;
				return start;
			}
		}
		start = allLines.length -2;
		return start;
	}

	private int getStartIndex(String[] allLines ) {
		int start = 0;
		for (int i = 0; i < allLines.length; i++) {
 			if (allLines[i].startsWith("Transaction")) {
				start = i;
				return start;
			}
		}
		return start;
	}
	 
  	public void storeFile() throws IOException {
		File[] files = new File(this.dir).listFiles();
		File fil = null;
 		fil = new File("D:\\Innovative Expenses\\2022filing\\barclays\\2855.csv");
	 	fil.createNewFile();
  		if (files != null && files.length > 0) {
			List<String> data = readCardData(files);
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
			log.info("No files for card" );
	}
  	public static void main(String arg[]) throws IOException
	{
		BarclayStatementReader reader = new BarclayStatementReader();
		reader.storeFile();
	}

}
