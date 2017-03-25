package parser.hw.dt.bean;

import java.util.Date;

/**
 * 用户手工上传日志表对应的实体类<br>
 * CltDtFileExplain
 * 
 * @author lijiayu
 * @date 2014年8月5日
 */
public class CltDtFileExplain{

	//	FILE_NAME	VARCHAR2(40)	N			文件名称
	//	USER_ID	VARCHAR2(20)	N			
	//	USER_NAME	VARCHAR2(100)	Y			用户中文名
	//	UPDATE_TIME	DATE	Y			上传时间
	//	AUTO	NUMBER(1)	Y		采集方式	1：自动采集。2：手工导入
	//	FILE_FTP_PATH	VARCHAR2(4000)	N		路测文件上传FTP路径	
	//	FILE_AUTO_2_DELETE	NUMBER(1)	N		手工上传文件是否被删除	1：未删除。2：已删除
	//	REMARK	VARCHAR2(255)	Y		"备注信息
	//	DEAL_COMPLETE	NUMBER	Y		0	采集是否解析完成。	0：未完成，1：完成

	/**
	 * 文件名称
	 */
	private String fileName;

	/**
	 * 用户id
	 */
	private String userId;

	/**
	 * 用户中文名
	 */
	private String userName;

	/**
	 * 上传时间
	 */
	private Date updateTime;

	/**
	 * 采集方式 1：自动采集。2：手工导入
	 */
	private int auto;

	/**
	 * 路测文件上传FTP路径
	 */
	private String fileFtpPath;

	/**
	 * 手工上传文件是否被删除 1：未删除。2：已删除
	 */
	private String fileAuto2Delete;

	/**
	 * 备注信息
	 */
	private String remark;

	/**
	 * 采集是否解析完成。 0：未完成，1：完成
	 */
	private int dealComplete;

	
	public String getFileName(){
		return fileName;
	}

	
	public void setFileName(String fileName){
		this.fileName = fileName;
	}

	public String getUserId(){
		return userId;
	}

	public void setUserId(String userId){
		this.userId = userId;
	}

	public String getUserName(){
		return userName;
	}

	public void setUserName(String userName){
		this.userName = userName;
	}

	public Date getUpdateTime(){
		return updateTime;
	}

	public void setUpdateTime(Date updateTime){
		this.updateTime = updateTime;
	}

	public int getAuto(){
		return auto;
	}

	public void setAuto(int auto){
		this.auto = auto;
	}

	public String getFileFtpPath(){
		return fileFtpPath;
	}

	public void setFileFtpPath(String fileFtpPath){
		this.fileFtpPath = fileFtpPath;
	}

	public String getFileAuto2Delete(){
		return fileAuto2Delete;
	}

	public void setFileAuto2Delete(String fileAuto2Delete){
		this.fileAuto2Delete = fileAuto2Delete;
	}

	public String getRemark(){
		return remark;
	}

	public void setRemark(String remark){
		this.remark = remark;
	}

	public int getDealComplete(){
		return dealComplete;
	}

	public void setDealComplete(int dealComplete){
		this.dealComplete = dealComplete;
	}
}
