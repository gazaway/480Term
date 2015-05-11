package comps;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;


public class ParseSingle {
	
	public void translate() throws IOException {
		String file = "reports.json";
		FileWriter fout = new FileWriter("_" + file);
		FileReader fin = new FileReader(file);
		Scanner scan = new Scanner(fin);
		scan.useDelimiter("},");
		
		while (scan.hasNext()) {
			String temp = scan.next();
			fout.write(temp + "},\n");
		}
		fout.close();
		scan.close();
	}
	
	public static void main(String[] args) throws IOException {
		ParseSingle tr = new ParseSingle();
		tr.translate();

	}

}
