package com.innovative.taxes.read;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

public abstract class PdfReader {
	private static org.apache.log4j.Logger log = LogManager.getLogger(PdfReader.class);
	
	public String[] read(File file) throws IOException {
		// Loading an existing document
		PDDocument document = PDDocument.load(file);
//	      //Instantiate PDFTextStripper class
		PDFTextStripper pdfStripper = new PDFTextStripper();
//	      //Retrieving text from PDF document
		String text = pdfStripper.getText(document);
		String[] texts = getDocument(text);

		 //log.info(text);
	//	System.out.println(text);
		// Closing the document
		document.close();
		document = null;
		return texts;
	}
	public List<String> readCardData(File[] files) throws IOException {
		List<String> data = new ArrayList<String>();
		String[] filedata = null;
		String str = "Reading file  ?";
		for (File file : files) {
			//data.add(str.replace("?", file.getAbsolutePath()));
			log.info(str.replace("?", file.getAbsolutePath()));
			filedata  =read(file);
			if(filedata !=null) {
				data.addAll(Arrays.asList(filedata));
			}
		}
		data.forEach(x -> System.out.println(x));
		return data;
	}


	protected abstract String[] getDocument(String text);
}
