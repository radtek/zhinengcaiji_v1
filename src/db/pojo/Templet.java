package db.pojo;

/**
 * 模板POJO
 * <P>
 * 对应IGP_CONF_TEMPLET表一条记录
 * </p>
 * 
 * @author YangJian
 * @since 1.0
 */
public class Templet {

	private int tmpID;

	private int tmpType;

	private String tmpName;

	private String edition;

	private String tempFileName;

	public Templet() {
		super();
	}

	public Templet(int tmpID, int tmpType, String tmpName, String edition, String tempFileName) {
		super();
		this.tmpID = tmpID;
		this.tmpType = tmpType;
		this.tmpName = tmpName;
		this.edition = edition;
		this.tempFileName = tempFileName;
	}

	public int getTmpID() {
		return tmpID;
	}

	public void setTmpID(int tmpID) {
		this.tmpID = tmpID;
	}

	public int getTmpType() {
		return tmpType;
	}

	public void setTmpType(int tmpType) {
		this.tmpType = tmpType;
	}

	public String getTmpName() {
		return tmpName;
	}

	public void setTmpName(String tmpName) {
		this.tmpName = tmpName;
	}

	public String getEdition() {
		return edition;
	}

	public void setEdition(String edition) {
		this.edition = edition;
	}

	public String getTempFileName() {
		return tempFileName;
	}

	public void setTempFileName(String tempFileName) {
		this.tempFileName = tempFileName;
	}

}
