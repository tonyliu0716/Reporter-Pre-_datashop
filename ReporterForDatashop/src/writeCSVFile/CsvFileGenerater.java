package writeCSVFile;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;

import com.opencsv.CSVWriter;


public class CsvFileGenerater {
	
	public static void csvFileGenerate(ArrayList<String[]> strs) {
		
		Calendar c = Calendar.getInstance();
		int month = c.get(Calendar.MONTH) + 1;
		int day = c.get(Calendar.DAY_OF_MONTH);
		String newCsv = "OpenEdxData_" + month + day + ".txt";
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(newCsv), '\t', CSVWriter.NO_QUOTE_CHARACTER);
			String[] firstLine = new String[] {"Anon Student Id", "Session Id", "Time", "Time Zone", 
					"Student Response Type", "Student Response Subtype", "Tutor Response Type", 
					"Tutor Response Subtype", "Level (Default)", "Problem Name", "Problem View", 
					"Step Name", "Attempt At Step", "Outcome", "Selection", "Action", "Input", "Feedback Text", "Feedback Classification", 
					"Help Level", "Total Num Hints", "Condition Name", "Condition Type", "KC (Default)", "KC Category (Default)", "School", "Class", "CF(Study)",
					"CF(Course)", "CF(Section)", "CF(Subsection)", "CF(Unit)", "CF(UserRuntimeId)", "CF(StudentPastelId)", 
					"CF(VideoURL)", "CF(VideoPosition)", "CF(UnitId)", "CF(Action)", "CF(Result)", "CF(Question)", "CF(Choices)", "CF(SeqNumber)"};
			writer.writeNext(firstLine);
			
			// start to write all the column values to csv file
			for(int i = 0; i < strs.size(); i++) {
				writer.writeNext(strs.get(i));
			}
			
			
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		
	}
	
	public static void writeTrackingLogToCSV(ArrayList<String[]> strs) {
		// find the existing csv file:
		Calendar c = Calendar.getInstance();
		int month = c.get(Calendar.MONTH) + 1;
		int day = c.get(Calendar.DAY_OF_MONTH);
		String csv = "OpenEdxData_" + month + day + ".txt";
		CSVWriter writer = null;
		try {
			// try to append the content but not override:
			writer = new CSVWriter(new FileWriter(csv, true), '\t', CSVWriter.NO_QUOTE_CHARACTER);
			
			// start to write all the column values to csv file:
			for(int i = 0; i < strs.size(); i++) {
				writer.writeNext(strs.get(i));
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			if(writer != null) {
				try{
					writer.close();
				} catch(IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	
	
}
