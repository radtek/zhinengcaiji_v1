package parser.hw.am;

/**
 * 对应 ne_bts_w表只取以下字段ne_bts_id, omcid, ne_bsc_id, t.bts_id, bts_name, city_id 其中ne_bts_id, bts_name, city_id是必须要有的字段 NE_BSC_W
 * 
 * @author liangww 2012-5-25
 * @version 1.0 1.0.1 liangww 2012-06-14 删除成员omcId, btsId, neBscId<br>
 * 
 */
public class NeBtsW {

	// private int omcId; //
	private int cityId = 0;  			//

	private String neBtsId = null;		//

	private String btsName = null;		//
	// private String btsId = null; //
	// private String neBscId = null; //

	public NeBtsW(int omcId, int cityId, String neBtsId, String btsName) {
		// this.omcId = omcId;
		this.cityId = cityId;
		this.neBtsId = neBtsId;
		this.btsName = btsName;
		// this.btsId = btsId;
		// this.neBscId = neBscId;
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

	public int getCityId() {
		return cityId;
	}

	public void setCityId(int cityId) {
		this.cityId = cityId;
	}

	public String getNeBtsId() {
		return neBtsId;
	}

	public void setNeBtsId(String neBtsId) {
		this.neBtsId = neBtsId;
	}

	public String getBtsName() {
		return btsName;
	}

	public void setBtsName(String btsName) {
		this.btsName = btsName;
	}

	// public String getBtsId()
	// {
	// return btsId;
	// }
	//
	// public void setBtsId(String btsId)
	// {
	// this.btsId = btsId;
	// }

	// public String getNeBscId()
	// {
	// return neBscId;
	// }
	//
	// public void setNeBscId(String neBscId)
	// {
	// this.neBscId = neBscId;
	// }

}
