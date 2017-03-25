package db.dao;

import java.util.ArrayList;
import java.util.List;

/**
 * 分页查询的结果
 * 
 * @author ChenSijiang
 * @param <T>
 *            所查询的数据类型
 */
public class PageQueryResult<T> {

	private int pageSize;

	private int currentPage;

	private int pageCount;

	private List<T> datas;

	public PageQueryResult() {
		datas = new ArrayList<T>();
	}

	public PageQueryResult(int pageSize, int currentPage, int pageCount, List<T> datas) {
		super();
		this.pageSize = pageSize;
		this.currentPage = currentPage;
		this.pageCount = pageCount;
		this.datas = datas;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getCurrentPage() {
		return currentPage;
	}

	public void setCurrentPage(int currentPage) {
		this.currentPage = currentPage;
	}

	public int getPageCount() {
		return pageCount;
	}

	public void setPageCount(int pageCount) {
		this.pageCount = pageCount;
	}

	public List<T> getDatas() {
		return datas;
	}

	public void setDatas(List<T> datas) {
		this.datas = datas;
	}

}
