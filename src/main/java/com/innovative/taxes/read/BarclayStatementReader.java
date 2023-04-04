package com.innovative.taxes.read;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

public class BarclayStatementReader extends PdfReader {
	private static Logger log = LogManager.getLogger(BarclayStatementReader.class);
	String dir = "D:\\Innovative Expenses\\2022filing\\barclays\\2855\\";
	
	public String[] getDocument(String text) {
		String[] lines = text.split("\n");
		int start = getStartIndex(lines);
		int end = getEndIndex(lines, start);
 		String subset[] =  Arrays.copyOfRange(lines, start, end + 1);
		return subset;
	}
	private int getEndIndex(String[] allLines, int start) {
	 	for (int i = start; i < allLines.length; i++) {
 			if (allLines[i].startsWith("This Year-to-date")) {
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
