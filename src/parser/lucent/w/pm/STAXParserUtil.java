package parser.lucent.w.pm;

final class STAXParserUtil {

	private STAXParserUtil() {
	}

	public static SNStruct parserSN(String sn) {
		try {
			String[] items = sn.split(",");
			String s1 = items[0].substring(items[0].indexOf("=") + 1).trim();
			String s2 = items[1].substring(items[1].indexOf("=") + 1).trim();
			String me = items[2].substring(items[2].indexOf("=") + 1).trim();
			return new SNStruct(s1, s2, me);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * sn节点内的信息
	 * 
	 * @author ChenSijiang 2010-9-9
	 */
	static class SNStruct {

		String subnetwork1; // 即rncname

		String subnetwork2; // 即subnetwork

		String managedElement; // 即managedelement

		public SNStruct(String subnetwork1, String subnetwork2, String managedElement) {
			super();
			this.subnetwork1 = subnetwork1;
			this.subnetwork2 = subnetwork2;
			this.managedElement = managedElement;
		}

	}

	public static void main(String[] args) {
		System.out.println(parserSN("subNetwork=hljwnms,subNetwork=utran,ManagedElement=RNC-DQRNC01"));
	}
}
