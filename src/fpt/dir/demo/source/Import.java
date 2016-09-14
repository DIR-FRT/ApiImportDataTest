package fpt.dir.demo.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Import {
	
	public static SQLHelper helper;	
	
	public static void main(String[] args) {
		String folderLink = args[0]; //D:/folder
		
		File folder = new File(folderLink);
		File[] listOfFile = folder.listFiles();
		helper = new SQLHelper(); 
		for (File file : listOfFile) {
			if (file.isFile()) {
				String fileUrl = file.getPath();
				String fileNameStr = file.getName();
				String fileName = fileNameStr.substring(0,fileNameStr.length() - 4);
				importLoginTable(fileUrl, fileName);
			}
		}
	}
	
	public void listFilesForFolder(final File folder) {
	    for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            listFilesForFolder(fileEntry);
	        } else {
	            System.out.println(fileEntry.getName());
	        }
	    }
	}
	
	public static void importLoginTable(String csvFile, String tableName)
	{
		BufferedReader br = null;
		String line = "";
		String csvSplitBy = ",";
		
		try {

            br = new BufferedReader(new FileReader(csvFile));
            String headerStr = br.readLine();
            String[] headerText = headerStr.split(csvSplitBy);
            
            addTableToPSQL(tableName, headerText);
            
            while ((line = br.readLine()) != null) {
            	
                String[] data = line.split(csvSplitBy);                
                helper.addDataToTable(tableName, headerText, data);
            }           
            

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
	}
	
	public static void addTableToPSQL(String tableName, String[] fields)
	{		
		if (!helper.isTableExist(tableName)) {
			helper.createNewTable(tableName, fields);
		}
		else {
			if(helper.isColumnChange(tableName, fields)) {
				helper.dropTable(tableName);
				helper.createNewTable(tableName, fields);
			}
			else {
				helper.deleteData(tableName);
			}
		}
	}
}
