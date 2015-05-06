package comps;
import java.io.File;
import java.io.FileWriter;
import java.util.Scanner;


public class transCompPerDay {

	public static void main(String[] args) throws Exception {
		String name = "part";
		File file = new File(name);
		FileWriter out = new FileWriter("chartCompDay");
		Scanner scan = new Scanner(file);
		while (scan.hasNextLine()){
			String line = scan.nextLine();
			if (line.contains("Ant") || line.contains("Compare") || line.contains("Core") || line.contains("Help") || line.contains("WebDAV") || line.contains("Team") || line.contains("Search")){
				out.write(line + '\n');
			}
		}
		out.close();
		scan.close();
	}
}
