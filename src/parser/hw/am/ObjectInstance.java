package parser.hw.am;

/**
 * 
 * ObjectInstance
 * 
 * @author
 * @version 1.0.0 1.0.1 liangww 2012-06-14 删除成员omcId, neName, objFdn<br>
 */
class ObjectInstance {

	// public int omcId;
	public String neFdn;

	public long objectTypeId;

	public long objectNo;

	// public String neName;
	// public String objFdn;

	public ObjectInstance() {
		super();
	}

	public ObjectInstance(String neFdn, long objectTypeId, long objectNo) {
		super();
		// this.omcId = omcId;
		this.neFdn = neFdn;
		this.objectTypeId = objectTypeId;
		this.objectNo = objectNo;
		// this.neName = neName;
		// this.objFdn = objFdn;
	}

	// public ObjectInstance(int omcId, String neFdn, long objectTypeId, long objectNo, String neName, String objFdn)
	// {
	// super();
	// // this.omcId = omcId;
	// this.neFdn = neFdn;
	// this.objectTypeId = objectTypeId;
	// this.objectNo = objectNo;
	// // this.neName = neName;
	// // this.objFdn = objFdn;
	// }

	// @Override
	// public String toString()
	// {
	// return "ObjectInstance [neFdn=" + neFdn + ", neName=" + neName
	// + ", objFdn=" + objFdn + ", objectNo=" + objectNo
	// + ", objectTypeId=" + objectTypeId + ", omcId=" + omcId + "]";
	// }

}
