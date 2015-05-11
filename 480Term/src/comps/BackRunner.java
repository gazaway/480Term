package comps;

public class BackRunner {

	public static void main(String[] args) {
		CommonComps cc = new CommonComps();
		try {
			//IN: args[0]
			//OUT: s3://480term/cc
			cc.run(args);
		} catch (Exception e) {
			System.out.println("Error running CommonComps");
			System.out.println(e);
		}
		CompTotPerDate ct = new CompTotPerDate();
		try {
			//IN: args[0]
			//OUT: s3://480term/cd
			ct.run(args);
		} catch (Exception e) {
			System.out.println("Error running CompTotPerDate");
			System.out.println(e);
		}
		MapRedCCTranslate mRCCT =  new MapRedCCTranslate();
		try {
			//IN: s3://480term/cc
			//OUT: args[1]
			mRCCT.run(args);
		} catch (Exception e) {
			System.out.println("Error running CompTotPerDate");
			System.out.println(e);
		}
		MapRedCDTranslate mRCDT =  new MapRedCDTranslate();
		try {
			//IN: s3://480term/cd
			//OUT: args[2]
			mRCDT.run(args);
		} catch (Exception e) {
			System.out.println("Error running CompTotPerDate");
			System.out.println(e);
		}
		System.exit(0);
	}

}
