package store;

import exception.StoreException;

/**
 * 存储接口
 * 
 * @author YangJian
 * @since 3.1
 */
public interface Store {

	public void open() throws StoreException;

	public void write(String data) throws StoreException;

	public void flush() throws StoreException;

	public void commit() throws StoreException;

	public void close();
}
