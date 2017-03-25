package parser.hw.am;

/**
 * 
 * MapHW
 * 
 * @version l.0 1.0.1 liangww 2012-05-29 删除neBscId，neMscId成员，增加cellName，btsName<br>
 *          1.0.2 liangww 2012-06-14 删除成员omcId, neBscId, neMscId<br>
 * 
 */
public class MapHW {

	// public int omcId;
	// public long objectTypeId;
	// public long objectNo;
	public int cityId;

	public String neCellId;

	public String neBtsId;

	// public String neBscId;
	// public String neMscId;
	public String version;

	public String cellName = null;		//

	public String btsName = null;		//

	public MapHW() {
		super();
	}

	public MapHW(String neCellId, String neBtsId, String version) {
		super();
		// this.omcId = omcId;
		// this.objectTypeId = objectTypeId;
		// this.objectNo = objectNo;
		// this.cityId = cityId;
		this.neCellId = neCellId;
		this.neBtsId = neBtsId;
		// this.neBscId = neBscId;
		// this.neMscId = neMscId;
		this.version = version;
	}

	// @Override
	// public String toString()
	// {
	// return "MapHW [cityId=" + cityId
	// + ", neBtsId=" + neBtsId + ", neCellId=" + neCellId
	// + ", objectNo=" + objectNo
	// + ", objectTypeId=" + objectTypeId + ", omcId=" + omcId
	// + ", version=" + version + "]";
	// }
}
