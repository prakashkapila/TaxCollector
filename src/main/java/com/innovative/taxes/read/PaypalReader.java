package com.innovative.taxes.read;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.io.Serializable;
import java.util.ArrayList;
import org.apache.log4j.LogManager;

public class PaypalReader extends PdfReader implements Serializable {
	
	private static final long serialVersionUID = 3826792989048545098L;
	private static org.apache.log4j.Logger log = LogManager.getLogger(PaypalReader.class);

	public String[] getDocument(String text) {
		String[] lines = text.split("\n");
		return getRecords(lines).toArray(new String[0]);
	}

	String fieldInd = "!$";

	private String getRecord(String[] lines) {
		StringBuilder sb = new StringBuilder();
		sb.append(lines[0].replace("Express Checkout Payment", "")).append(fieldInd).append(lines[1]).append(fieldInd)
				.append(lines[2].replace("Checking x-3045", "").trim());
		return sb.toString().replace(",", "").replace("\r", "").replace(fieldInd, ",");
	}

	private List<String> getRecords(String[] lines) {
		List<String> ret = new ArrayList<>();
		String rec = "";
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].contains("Express Checkout Payment")) {
				ret.add(getRecord(Arrays.copyOfRange(lines, i, i + 4)));
				i += 4;
			}
			if (lines[i].contains("PreApproved Payment Bill User Payment")) {
				rec = getRecord(Arrays.copyOfRange(lines, i + 1, i + 4));
				if (rec.contains("Cue")) {
					rec += lines[i + 5] + " ";
				}
				ret.add(rec);
				i += 4;
			}
		}
		return ret;
	}

	private int getEndIndex(String[] allLines, int start) {
		start = allLines.length - 2;
		return start;
	}

	private int getStartIndex(String[] allLines) {
		int start = 0;
		for (int i = 0; i < allLines.length; i++) {
			if (allLines[i].startsWith("DATE DESCRIPTION CURRENCY AMOUNT FEES TOTAL")) {
				start = i;
				return start;
			}
		}
		return start;
	}

	public void storeFiles() {
		try {
			storeFile();

		} catch (Exception esp) {
			esp.printStackTrace();
		}
	}

	private File[] getFiles() {
		File dir = new File("D:\\Innovative Expenses\\2022filing\\paypal\\stmts");
		return dir.listFiles();

	}

	public void storeFile() throws IOException {
		File[] files = null;
		File fil = null;
		files = getFiles();
		fil = new File("D:\\Innovative Expenses\\2022filing\\paypal\\paypal.csv");
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
			log.info("No files for card");
	}

	public static void main(String arg[]) {
		PaypalReader reader = new PaypalReader();
		reader.storeFiles();
	}

}
