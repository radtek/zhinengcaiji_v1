package parser.lucent.cm;

import java.util.SortedMap;

class ALParamRecord {

	String omcId;

	String collecttime;

	String stamptime;

	String id;

	String pId;

	String pTableName;

	String rncId1;

	String nodebId1;

	String fddCellId1;

	String ci1;

	String lac1;

	SortedMap<String, String> values;

	public ALParamRecord(String omcId, String collecttime, String stamptime, String id, String pId, String pTableName, String rncId1,
			String nodebId1, String fddCellId1, String ci1, String lac1, SortedMap<String, String> values) {
		super();
		this.omcId = omcId;
		this.collecttime = collecttime;
		this.stamptime = stamptime;
		this.id = id;
		this.pId = pId;
		this.pTableName = pTableName;
		this.rncId1 = rncId1;
		this.nodebId1 = nodebId1;
		this.fddCellId1 = fddCellId1;
		this.ci1 = ci1;
		this.lac1 = lac1;
		this.values = values;
	}

	@Override
	public String toString() {
		return "Record [ci1=" + ci1 + ", collecttime=" + collecttime + ", fddCellId1=" + fddCellId1 + ", id=" + id + ", lac1=" + lac1 + ", nodebId1="
				+ nodebId1 + ", omcId=" + omcId + ", pId=" + pId + ", pTableName=" + pTableName + ", rncId1=" + rncId1 + ", stamptime=" + stamptime
				+ ", values=" + values + "]";
	}

}
