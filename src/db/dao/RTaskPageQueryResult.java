package db.dao;

import java.util.List;

@SuppressWarnings("rawtypes")
public class RTaskPageQueryResult<T> extends PageQueryResult {

	private int recordCount;

	@SuppressWarnings("unchecked")
	public RTaskPageQueryResult(int pageSize, int currentPage, int pageCount, List<T> datas) {
		super(pageSize, currentPage, pageCount, datas);
	}

	public RTaskPageQueryResult(int pageSize, int currentPage, int pageCount, int recordCount, List<T> datas) {
		this(pageSize, currentPage, pageCount, datas);
		this.recordCount = recordCount;
	}

	public String getPageInfo() {
		StringBuilder sb = new StringBuilder("<span style='float: left'>共");
		sb.append(recordCount + "条</span>");
		sb.append("<span style='float: right'> <a href='#' id='firstPage'>|&lt;&lt;首页</a>&nbsp;&nbsp;&nbsp;");
		sb.append("<a href='#' id='frontPage'>&lt;&lt;上一页</a>&nbsp;&nbsp;&nbsp; ");
		sb.append("<a href='#' id='nextPage'>下一页&gt;&gt;</a>&nbsp;&nbsp;&nbsp; ");
		sb.append("<a href='#' id='lastPage'>末页&gt;&gt;|</a>&nbsp;&nbsp;&nbsp;");
		sb.append("当前第 <label id='firPageLabel'>");
		sb.append(getCurrentPage() + "</label> 页/");
		sb.append("共 <label id='totalPageLabel'>" + getPageCount());
		sb.append("</label> 页&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
		sb.append("到第 <input type='text' size='2' id='pageNum' onKeyPress='return event.keyCode>=48&&event.keyCode<=57'>页 ");
		sb.append("<a href='#' id='currentPage'>go&nbsp;&nbsp;&nbsp;</a> ");
		sb.append("<input name='pageSize' type='text' id='pageSize' ");
		sb.append("onKeyPress='return event.keyCode>=48&&event.keyCode<=57' ");
		sb.append("value='" + getPageSize());
		sb.append("' size='1' maxlength='3' /> 条/页 &nbsp;&nbsp;&nbsp;</span>");
		return sb.toString();
	}

	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

}
