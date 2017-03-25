package parser.hw.am;

import java.io.Serializable;

/**
 * 包装通过查询cfg_map_hw_ne_objectno、cfg_map_hw_ne_objectno表所得到的信息。
 * 
 * @author ChenSijiang * @version 1.0.0 1.0.1 liangww 2012-07-27 增加neBscId，neBtsId， neCellId， bscName， btsName，cellName<br>
 * 
 */
public class QueriedEntry implements Serializable {

	public int cityId;

	public String version;

	public String neSysid; //

	public String neName; //

	public String neBscId = null; // bsc id

	public String neBtsId = null; //

	public String neCellId = null; //

	public String bscName = null; //

	public String btsName = null; //

	public String cellName = null; //

	public QueriedEntry() {
		super();
	}

	public QueriedEntry(int cityId, String version, String neSysid, String neName) {
		super();
		this.cityId = cityId;
		this.version = version;
		this.neSysid = neSysid;
		this.neName = neName;
	}

	@Override
	public String toString() {
		return "QueriedEntry [cityId=" + cityId + ", neName=" + neName + ", neSysid=" + neSysid + ", version=" + version + "]";
	}

	/**
	 * 
	 * @param neLeven
	 */
	public void init(int neLeven) {
		switch (neLeven) {
			case MappingTables.CELL_LEVEL :
				neSysid = neCellId;
				neName = cellName;
				break;

			case MappingTables.BTS_LEVEL :
				neSysid = neBtsId;
				neName = btsName;
				break;

			case MappingTables.BSC_LEVEL :
				neSysid = neBscId;
				neName = bscName;
				break;

			default :
				break;
		}

	}

}
