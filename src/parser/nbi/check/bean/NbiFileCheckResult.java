package parser.nbi.check.bean;

import java.util.Date;

/**
 * @author yuy 表MOD_NBI_FILE_CHECK_RESULT对应的实体
 */
public class NbiFileCheckResult {

	/** 核查时间 */
	public Date START_TIME;

	/** 核查省份 */
	public String PROVINCE_NAME;

	/** 文件编号 */
	public Integer FILE_ID;

	/** 集团上报对应的文件名 */
	public String FILE_NAME;

	/** 核查类型 */
	public Integer FILE_CHECK_TYPE;

	/** 上报文件核查结果 */
	public String FILE_CHECK_RESULT;

	/**
	 * @return START_TIME
	 */
	public Date getSTART_TIME() {
		return START_TIME;
	}

	/**
	 * @param sTART_TIME
	 */
	public void setSTART_TIME(Date sTART_TIME) {
		START_TIME = sTART_TIME;
	}

	/**
	 * @return pROVINCE_NAME
	 */
	public String getPROVINCE_NAME() {
		return PROVINCE_NAME;
	}

	/**
	 * @param pROVINCE_NAME
	 */
	public void setPROVINCE_NAME(String pROVINCE_NAME) {
		PROVINCE_NAME = pROVINCE_NAME;
	}

	/**
	 * @return fILE_ID
	 */
	public Integer getFILE_ID() {
		return FILE_ID;
	}

	/**
	 * @param fILE_ID
	 */
	public void setFILE_ID(Integer fILE_ID) {
		FILE_ID = fILE_ID;
	}

	/**
	 * @return fILE_NAME
	 */
	public String getFILE_NAME() {
		return FILE_NAME;
	}

	/**
	 * @param fILE_NAME
	 */
	public void setFILE_NAME(String fILE_NAME) {
		FILE_NAME = fILE_NAME;
	}

	/**
	 * @return fILE_CHECK_TYPE
	 */
	public Integer getFILE_CHECK_TYPE() {
		return FILE_CHECK_TYPE;
	}

	/**
	 * @param fILE_CHECK_TYPE
	 */
	public void setFILE_CHECK_TYPE(Integer fILE_CHECK_TYPE) {
		FILE_CHECK_TYPE = fILE_CHECK_TYPE;
	}

	/**
	 * @return fILE_CHECK_RESULT
	 */
	public String getFILE_CHECK_RESULT() {
		return FILE_CHECK_RESULT;
	}

	/**
	 * @param fILE_CHECK_RESULT
	 */
	public void setFILE_CHECK_RESULT(String fILE_CHECK_RESULT) {
		FILE_CHECK_RESULT = fILE_CHECK_RESULT;
	}
}
