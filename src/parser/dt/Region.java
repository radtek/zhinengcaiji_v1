package parser.dt;

/**
 * 场景和REGION实体类<br>
 * 
 * @author chenrongqiang @ 2013-10-17
 */
public class Region {
	
	/**
	 * PIECE名称
	 */
	private String pieceName;

	/**
	 * REGION ID
	 */
	private long regionId;

	/**
	 * REGION名称
	 */
	private String regionName;

	/**
	 * @return the regionId
	 */
	public long getRegionId() {
		return regionId;
	}

	/**
	 * @param regionId the regionId to set
	 */
	public void setRegionId(long regionId) {
		this.regionId = regionId;
	}

	/**
	 * @return the regionName
	 */
	public String getRegionName() {
		return regionName;
	}

	/**
	 * @param regionName the regionName to set
	 */
	public void setRegionName(String regionName) {
		this.regionName = regionName;
	}

	/**
	 * @return
	 */
	public String getPieceName() {
		return pieceName;
	}

	
	/**
	 * @param pieceName
	 */
	public void setPieceName(String pieceName) {
		this.pieceName = pieceName;
	}

}
