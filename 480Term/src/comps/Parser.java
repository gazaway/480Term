package comps;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;



public class Parser {
	public void translate(String directory) throws IOException {
		File folder = new File(directory);
		File[] list = folder.listFiles();
		
		for (File file : list) {
			if (file.isFile() && file.getName().contains(".json") && !file.getName().contains("reports")){
				FileWriter fout = new FileWriter(file.getName().replaceAll(".json", "") + "_TR.json");
				FileReader fin = new FileReader(file);
				Scanner scan = new Scanner(fin);
				scan.useDelimiter("}],");
				
				while (scan.hasNext()) {
					String temp = scan.next();
					fout.write(temp + "}],\n");
				}
				fout.close();
				scan.close();
			}
		}
		folder = new File(directory);
		list = folder.listFiles();
		
		for (File file : list) {
			if (file.getName().contains(".json") && !file.getName().contains("_TR.json") && !file.getName().contains("reports")){
				file.delete();
			}
		}
	}
	
	public static void main(String args[]) throws IOException {
		Parser tr = new Parser();
		tr.translate(args[0]);
	}
}
