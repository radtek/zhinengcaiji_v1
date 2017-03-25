package parser.hw.am;

/**
 * 对应 ne_bsc_w表只取以下字段 ne_bsc_id, bsc_name , omcid, version 其中ne_bsc_id, version是必须要有的字段 NE_BSC_W
 * 
 * @author liangww 2012-5-25
 * @verion 1.0 1.0.1 liangww 2012-06-14 删除omcId<br>
 * 
 */
class NeBscW {

	// private int omcId =0;
	private String neBscId = null;

	private String bscName = null;

	private String version = null;

	public NeBscW(int omcId, String neBscId, String bscName, String version) {
		// this.omcId = omcId;
		this.neBscId = neBscId;
		this.bscName = bscName;
		this.version = version;
	}

	// public int getOmcId()
	// {
	// return omcId;
	// }
	//
	// public void setOmcId(int omcId)
	// {
	// this.omcId = omcId;
	// }

	public String getNeBscId() {
		return neBscId;
	}

	public void setNeBscId(String neBscId) {
		this.neBscId = neBscId;
	}

	public String getBscName() {
		return bscName;
	}

	public void setBscName(String bscName) {
		this.bscName = bscName;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

}
