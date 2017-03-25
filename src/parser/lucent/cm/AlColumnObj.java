package parser.lucent.cm;

public class AlColumnObj {

	public Integer omcId;

	public String strCollecttime;

	public String strStamptime;

	public String id;

	public String pid;

	public String ptablename;

	public String rncid;

	public String nodebid;

	public String fddcellid;

	public String ci;

	public String lac;

	public String[] pids;

	public Integer getOmcId() {
		return omcId;
	}

	public void setOmcId(Integer omcId) {
		this.omcId = omcId;
	}

	public String getStrCollecttime() {
		return strCollecttime;
	}

	public void setStrCollecttime(String strCollecttime) {
		this.strCollecttime = strCollecttime;
	}

	public String getStrStamptime() {
		return strStamptime;
	}

	public void setStrStamptime(String strStamptime) {
		this.strStamptime = strStamptime;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getPtablename() {
		return ptablename;
	}

	public void setPtablename(String ptablename) {
		this.ptablename = ptablename;
	}

	public String getRncid() {
		return rncid;
	}

	public void setRncid(String rncid) {
		this.rncid = rncid;
	}

	public String getNodebid() {
		return nodebid;
	}

	public void setNodebid(String nodebid) {
		this.nodebid = nodebid;
	}

	public String getFddcellid() {
		return fddcellid;
	}

	public void setFddcellid(String fddcellid) {
		this.fddcellid = fddcellid;
	}

	public String getCi() {
		return ci;
	}

	public void setCi(String ci) {
		this.ci = ci;
	}

	public String getLac() {
		return lac;
	}

	public void setLac(String lac) {
		this.lac = lac;
	}

	public String[] getPids() {
		return pids;
	}

	public void setPids(String[] pids) {
		String[] strs = new String[pids.length];
		System.arraycopy(pids, 0, strs, 0, pids.length);
		this.pids = strs;
	}
}
