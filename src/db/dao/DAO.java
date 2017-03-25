package db.dao;

import java.util.List;

/**
 * 对每个实体类的增,删,查,改
 * 
 * @author IGP TDT
 * @since 1.0
 */
public interface DAO<T> {

	public int add(T entity);

	public boolean update(T entity);

	public boolean delete(T entity);

	public boolean delete(long id);

	public List<T> list();

	public T getById(long id);

	public T getByName(String name);

	public List<T> criteriaQuery(T dev);

	public List<T> query(String sql);

	public boolean exists(T entity);

	public boolean validate(T entity);

	public PageQueryResult<T> pageQuery(int pageSize, int currentPage);
}
