package com.innovative.taxes.read;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.LogManager;

public class ChasePdfStatementReader extends PdfReader {
	private static org.apache.log4j.Logger log = LogManager.getLogger(ChasePdfStatementReader.class);
	
	public String[] getDocument(String text) {
		String[] lines = text.split("\n");
		int start = getStartIndex(lines);
		int end = getEndIndex(lines, start);
 		String subset[] =  Arrays.copyOfRange(lines, start, end + 1);
		return subset;
	}
	private int getEndIndex(String[] allLines, int start) {
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
	public void storeFiles() {
		try {
			storeFile(2662);
			storeFile(3908);
		 
		} catch (Exception esp) {
			esp.printStackTrace();
		}
	}
	private File[] getFiles(int card) {
		 	switch (card) {
			case 2662: {
				File dir = new File("D:\\Innovative Expenses\\2022filing\\chase\\2662\\");
				return dir.listFiles();
			}
			case 3908: {
				File dir = new File("D:\\Innovative Expenses\\2022filing\\chase\\9390");
				return dir.listFiles();
			}
			}
		
		return null;
	}
	
	public void storeFile(int card) throws IOException {
		File[] files = null;
		File fil = null;
		switch (card) {
		case 2662: {
			files = getFiles(2662);
			fil = new File("D:\\Innovative Expenses\\2022filing\\chase\\2662.csv");
		 	fil.createNewFile();
			break;
		}
		case 3908: {
			files = getFiles(3908);
			fil = new File("D:\\Innovative Expenses\\2022filing\\chase\\3908.csv");
			fil.createNewFile();
			break;
		}
		}
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
			log.info("No files for card" + card);
	}
	
	public static void main(String arg[])
	{
		ChasePdfStatementReader reader = new ChasePdfStatementReader();
		reader.storeFiles();
	}
}
