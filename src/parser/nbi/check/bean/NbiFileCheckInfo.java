package parser.nbi.check.bean;

/**
 * @author yuy 表CFG_NBI_FILE_CHECK对应的实体
 */
public class NbiFileCheckInfo {

	/** 文件编号 */
	public Integer FILE_ID;

	/** 文件匹配规则名称 */
	public String FILE_RULE_NAME;

	/** FTP对应的文件目录 */
	public String FTP_DIR;

	/** 样例文件名 */
	public String FILE_EXAMPLE_NAME;

	/** 厂家 */
	public String VENDOR;

	/** 功能类型 */
	public Integer FUNCTION_TYPE;

	/** 规范章节 */
	public String FILE_GROUP_CHAPTER_NAME;

	/** 规范名称 */
	public String FILE_GROUP_REGULATE_NAME;

	/** 是否开启检查项 */
	public Integer IS_EFFECT;

	/**
	 * @return FILE_ID
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
	 * @return FILE_RULE_NAME
	 */
	public String getFILE_RULE_NAME() {
		return FILE_RULE_NAME;
	}

	/**
	 * @param fILE_RULE_NAME
	 */
	public void setFILE_RULE_NAME(String fILE_RULE_NAME) {
		FILE_RULE_NAME = fILE_RULE_NAME;
	}

	/**
	 * @return fTP_DIR
	 */
	public String getFTP_DIR() {
		return FTP_DIR;
	}

	/**
	 * @param fTP_DIR
	 */
	public void setFTP_DIR(String fTP_DIR) {
		FTP_DIR = fTP_DIR;
	}

	/**
	 * @return fILE_EXAMPLE_NAME
	 */
	public String getFILE_EXAMPLE_NAME() {
		return FILE_EXAMPLE_NAME;
	}

	/**
	 * @param fILE_EXAMPLE_NAME
	 */
	public void setFILE_EXAMPLE_NAME(String fILE_EXAMPLE_NAME) {
		FILE_EXAMPLE_NAME = fILE_EXAMPLE_NAME;
	}

	/**
	 * @return vENDOR
	 */
	public String getVENDOR() {
		return VENDOR;
	}

	/**
	 * @param vENDOR
	 */
	public void setVENDOR(String vENDOR) {
		VENDOR = vENDOR;
	}

	/**
	 * @return fUNCTION_TYPE
	 */
	public Integer getFUNCTION_TYPE() {
		return FUNCTION_TYPE;
	}

	/**
	 * @param fUNCTION_TYPE
	 */
	public void setFUNCTION_TYPE(Integer fUNCTION_TYPE) {
		FUNCTION_TYPE = fUNCTION_TYPE;
	}

	/**
	 * @return FILE_GROUP_CHAPTER_NAME
	 */
	public String getFILE_GROUP_CHAPTER_NAME() {
		return FILE_GROUP_CHAPTER_NAME;
	}

	/**
	 * @param fILE_GROUP_CHAPTER_NAME
	 */
	public void setFILE_GROUP_CHAPTER_NAME(String fILE_GROUP_CHAPTER_NAME) {
		FILE_GROUP_CHAPTER_NAME = fILE_GROUP_CHAPTER_NAME;
	}

	/**
	 * @return fILE_GROUP_REGULATE_NAME
	 */
	public String getFILE_GROUP_REGULATE_NAME() {
		return FILE_GROUP_REGULATE_NAME;
	}

	/**
	 * @param fILE_GROUP_REGULATE_NAME
	 */
	public void setFILE_GROUP_REGULATE_NAME(String fILE_GROUP_REGULATE_NAME) {
		FILE_GROUP_REGULATE_NAME = fILE_GROUP_REGULATE_NAME;
	}

	/**
	 * @return IS_EFFECT
	 */
	public Integer getIS_EFFECT() {
		return IS_EFFECT;
	}

	/**
	 * @param iS_EFFECT
	 */
	public void setIS_EFFECT(Integer iS_EFFECT) {
		IS_EFFECT = iS_EFFECT;
	}

}
