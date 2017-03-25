package parser.hw.am;

public class SummarizingTest
{
	
	public static void main(String[] args)
	{
		int omcId = 101;
		int neLevel = MappingTables.CELL_LEVEL;
		QueriedEntry qu = null;
		
		//null
		qu = Summarizing.getHW3GM2000M1(omcId, neLevel, ".3221229568.3221233664.3221283366.3223584872");
		System.out.println(qu == null);
		//null
		qu = Summarizing.getHW3GM2000M1(omcId, neLevel, ".3221229568.3221233664.3221283366.3223584872");
		System.out.println(qu == null);
	
		//
		qu = Summarizing.getHW3GM2000M1(omcId, neLevel, ".3221229568.3221233664.3221284598.3224080813.3224129283");
		System.out.println(qu != null);
		//
		qu = Summarizing.getHW3GM2000M1(omcId, neLevel, ".3221229568.3221233664.3221284598.3224080813.3224129283");
		System.out.println(qu != null);
		
		//null
		qu = Summarizing.getHW3GM2000M2(821020324, "GZRNC05", "");
		System.out.println(qu == null);
		//cache null
		qu = Summarizing.getHW3GM2000M2(821020324, "GZRNC05", "");
		System.out.println(qu == null);
		
		// 
		qu = Summarizing.getHW3GM2000M2(821020324, "GZRNC05", "2000");
		System.out.println(qu != null);
		// cache 
		qu = Summarizing.getHW3GM2000M2(821020324, "GZRNC05", "2000");
		System.out.println(qu != null);
	}
}
