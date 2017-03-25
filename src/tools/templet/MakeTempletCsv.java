package tools.templet;

/**
 * 生成csv模板文件 MakeTempletCsv
 * 
 * @author liuwx 2010-8-17
 */
public class MakeTempletCsv {

	private String filePath;

	private String outfile;

	private String split;

	private String ftpPath;

	private String endcontent;

	private int varchar2Length;

	private String tableprefix;

	public MakeTempletCsv() {
		super();
	}

	public MakeTempletCsv(String filePath, String outfile, String split, String ftpPath, String endcontent, int varchar2Length, String tableprefix) {
		super();
		this.filePath = filePath;
		this.outfile = outfile;
		this.split = split;
		this.ftpPath = ftpPath;
		this.endcontent = endcontent;
		this.varchar2Length = varchar2Length;
		this.tableprefix = tableprefix;
	}

	public String getTableprefix() {
		return tableprefix;
	}

	public void setTableprefix(String tableprefix) {
		this.tableprefix = tableprefix;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getOutfile() {
		return outfile;
	}

	public void setOutfile(String outfile) {
		this.outfile = outfile;
	}

	public String getSplit() {
		return split;
	}

	public void setSplit(String split) {
		this.split = split;
	}

	public String getFtpPath() {
		return ftpPath;
	}

	public void setFtpPath(String ftpPath) {
		this.ftpPath = ftpPath;
	}

	public String getEndcontent() {
		return endcontent;
	}

	public void setEndcontent(String endcontent) {
		this.endcontent = endcontent;
	}

	public int getVarchar2Length() {
		return varchar2Length;
	}

	public void setVarchar2Length(int varchar2Length) {
		this.varchar2Length = varchar2Length;
	}

}
