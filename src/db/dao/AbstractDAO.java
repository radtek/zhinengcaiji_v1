package db.dao;

import java.util.List;

import org.apache.log4j.Logger;

import util.LogMgr;

/**
 * 抽象DAO
 * 
 * @author YangJian
 * @since 1.0
 */
public class AbstractDAO<T> implements DAO<T> {

	protected static Logger logger = LogMgr.getInstance().getSystemLogger();

	public AbstractDAO() {
		super();
	}

	@Override
	public int add(T entity) {
		return 0;
	}

	@Override
	public boolean delete(T entity) {
		return false;
	}

	@Override
	public boolean delete(long id) {
		return false;
	}

	@Override
	public T getById(long id) {
		return null;
	}

	@Override
	public T getByName(String name) {
		return null;
	}

	@Override
	public List<T> list() {
		return null;
	}

	@Override
	public PageQueryResult<T> pageQuery(int pageSize, int currentPage) {
		return null;
	}

	@Override
	public List<T> query(String sql) {
		return null;
	}

	@Override
	public boolean update(T entity) {
		return false;
	}

	@Override
	public boolean validate(T entity) {
		return false;
	}

	/**
	 * 删除所有表中的记录
	 * 
	 * @return 影响的行数
	 */
	public int clearAll() {
		return 0;
	}

	@Override
	public boolean exists(T entity) {
		return false;
	}

	@Override
	public List<T> criteriaQuery(T dev) {
		return null;
	}
}
