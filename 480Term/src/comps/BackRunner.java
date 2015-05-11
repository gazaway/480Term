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
			e.printStackTrace();
		}
		CompTotPerDate ct = new CompTotPerDate();
		try {
			//IN: args[0]
			//OUT: s3://480term/cd
			ct.run(args);
		} catch (Exception e) {
			System.out.println("Error running CompTotPerDate");
			e.printStackTrace();
		}
		MapRedCCTranslate mRCCT =  new MapRedCCTranslate();
		try {
			//IN: s3://480term/cc & s3://480term/cd
			//OUT: args[1]
			mRCCT.run(args);
		} catch (Exception e) {
			System.out.println("Error running CompTotPerDate");
			e.printStackTrace();
		}
		System.exit(0);
	}

}
