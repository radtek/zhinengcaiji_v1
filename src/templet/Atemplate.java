package templet;

import java.util.ArrayList;

import parser.Parser;

import task.CollectObjInfo;

/**
 * 按模板解析抽象类
 * 
 * @author miniz
 */
public abstract class Atemplate {

	protected CollectObjInfo m_TaskInfo;

	protected Parser m_ParseData = null;

	private ArrayList<String> filelist;

	protected void parse() {

	}

	public ArrayList<String> getFilelist() {
		return filelist;
	}

	public void setFilelist(ArrayList<String> filelist) {
		this.filelist = filelist;
	}

	public CollectObjInfo getM_TaskInfo() {
		return m_TaskInfo;
	}

	public void setM_TaskInfo(CollectObjInfo taskInfo) {
		m_TaskInfo = taskInfo;
	}

	public Parser getM_ParseData() {
		return m_ParseData;
	}

	public void setM_ParseData(Parser parseData) {
		m_ParseData = parseData;
	}

}
