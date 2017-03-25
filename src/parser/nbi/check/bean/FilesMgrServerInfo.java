package parser.nbi.check.bean;

/**
 * @author yuy 表mod_filesmgr_serverinfo对应的实体（各省市FTP信息）
 */
public class FilesMgrServerInfo {

	/** 协议 */
	public String PROTOCAL;

	/** FTP IP */
	public String SERVERADDRESS;

	/** FTP 端口 */
	public Integer SERVERPORT;

	/** FTP根目录 */
	public String SERVERDIRECTORY;

	/** FTP 用户名 */
	public String USERNAME;

	/** FTP 密码 */
	public String PASSWORD;

	/** 省市 代号 */
	public String CHINA_NAME;

	/**
	 * @return PROTOCAL
	 */
	public String getPROTOCAL() {
		return PROTOCAL;
	}

	/**
	 * @param pROTOCAL
	 */
	public void setPROTOCAL(String pROTOCAL) {
		PROTOCAL = pROTOCAL;
	}

	/**
	 * @return sERVERADDRESS
	 */
	public String getSERVERADDRESS() {
		return SERVERADDRESS;
	}

	/**
	 * @param sERVERADDRESS
	 */
	public void setSERVERADDRESS(String sERVERADDRESS) {
		SERVERADDRESS = sERVERADDRESS;
	}

	/**
	 * @return sERVERPORT
	 */
	public Integer getSERVERPORT() {
		return SERVERPORT;
	}

	/**
	 * @param sERVERPORT
	 */
	public void setSERVERPORT(Integer sERVERPORT) {
		SERVERPORT = sERVERPORT;
	}

	/**
	 * @return sERVERDIRECTORY
	 */
	public String getSERVERDIRECTORY() {
		return SERVERDIRECTORY;
	}

	/**
	 * @param sERVERDIRECTORY
	 */
	public void setSERVERDIRECTORY(String sERVERDIRECTORY) {
		SERVERDIRECTORY = sERVERDIRECTORY;
	}

	/**
	 * @return uSERNAME
	 */
	public String getUSERNAME() {
		return USERNAME;
	}

	/**
	 * @param uSERNAME
	 */
	public void setUSERNAME(String uSERNAME) {
		USERNAME = uSERNAME;
	}

	/**
	 * @return pASSWORD
	 */
	public String getPASSWORD() {
		return PASSWORD;
	}

	/**
	 * @param pASSWORD
	 */
	public void setPASSWORD(String pASSWORD) {
		PASSWORD = pASSWORD;
	}

	/**
	 * @return cHINA_NAME
	 */
	public String getCHINA_NAME() {
		return CHINA_NAME;
	}

	/**
	 * @param cHINA_NAME
	 */
	public void setCHINA_NAME(String cHINA_NAME) {
		CHINA_NAME = cHINA_NAME;
	}
}
