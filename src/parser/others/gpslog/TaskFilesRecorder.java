package parser.others.gpslog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import util.Util;

/**
 * 任务文件记录器，用于记录任务中的文件名，记录的方式主要是通过输出到tfr.dat文件中， 支持自动清除功能，其中maxRecordFileNum用于控制最大的文件记录个数， minRecordFileNum 用于控制当文件记录要清除时，必须保留的个数
 * 
 * @author liangww
 * @version 1.0
 * @create 2012-7-27 下午02:36:51
 */
public class TaskFilesRecorder {

	private String rootPath = null;		// 根目录

	private int maxRecordFileNum = 10000;		// 允许最大的文件个数（这个是记录的）

	private int minRecordFileNum = 5000;		// 最小的文件个数，用于裁减文件个数时用的

	private Map<Long, TaskFiles> map = new HashMap<Long, TaskFiles>();

	public TaskFilesRecorder(String rootPath) {
		this(rootPath, 10000, 5000);
	}

	public TaskFilesRecorder(String rootPath, int maxFileNum, int minFileNum) {
		this.rootPath = rootPath;
		this.maxRecordFileNum = maxFileNum;
		this.minRecordFileNum = minFileNum;
	}

	/**
	 * 加载缓存文件
	 * 
	 * @param taskId
	 */
	public synchronized boolean loadCache(long taskId) throws Exception {
		TaskFiles cache = this.map.get(taskId);
		if (cache == null) {
			cache = new TaskFiles();
			map.put(taskId, cache);
			cache.tfrPath = getPath(rootPath, taskId);
			cache.taskId = taskId;
			File file = new File(cache.tfrPath);
			// 如果是存在
			if (file.exists()) {
				cache.fileNameSet.addAll(load(file));
			}
			// 如果没有就创建目录
			else {
				// 创建父目录
				file.getParentFile().mkdirs();
				file.createNewFile();
			}

			//
			cache.writer = new FileWriter(file, true);
			return true;
		}

		return false;
	}

	/**
	 * 是否包括task的文件名fileName
	 * 
	 * @param taskId
	 * @param fileName
	 * @return
	 */
	public synchronized boolean containFileName(long taskId, String fileName) {
		TaskFiles cache = this.map.get(taskId);
		return cache != null && cache.fileNameSet.contains(fileName.toLowerCase());
	}

	/**
	 * 增加task的相关的fileName，同时写到文件中
	 * 
	 * @param taskId
	 * @param fileName
	 */
	public synchronized boolean addFileName(long taskId, String fileName) throws Exception {
		TaskFiles cache = this.map.get(taskId);
		if (cache == null) {
			return false;
		}

		if (cache.fileNameSet.size() + 1 > maxRecordFileNum) {
			// 先关了
			Util.closeCloseable(cache.writer);
			// 再读出来
			List<String> fiList = load(new File(cache.tfrPath));
			int begin = fiList.size() - minRecordFileNum;
			List<String> subList = fiList.subList(begin, fiList.size());

			// 加载内存map中
			cache.fileNameSet.clear();
			cache.fileNameSet.addAll(subList);

			// 写到文件中
			cache.writer = new FileWriter(cache.tfrPath, false);
			for (int i = 0; i < subList.size(); i++) {
				cache.writer.write(subList.get(i) + "\r\n");
			}
		}

		cache.fileNameSet.add(fileName.toLowerCase());
		// 修改写文件时，不转成小写的bug--liangww 2012-10-25
		cache.writer.write(fileName.toLowerCase() + "\r\n");
		cache.writer.flush();

		return true;
	}

	/**
	 * 关闭
	 */
	public synchronized void close() {
		Iterator<TaskFiles> itr = map.values().iterator();
		while (itr.hasNext()) {
			TaskFiles cache = itr.next();
			cache.fileNameSet.clear();
			Util.closeCloseable(cache.writer);
		}

		map.clear();
	}

	/**
	 * 加载
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	static List<String> load(File file) throws IOException {
		List<String> lines = new ArrayList<String>();

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line = null;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
		} finally {
			Util.closeCloseable(reader);
		}

		return lines;
	}

	static String getPath(String rootPath, long taskId) {
		return rootPath + "/" + taskId + "/tfr.dat";
	}

	class TaskFiles {

		long taskId = 0;	// 任务id

		FileWriter writer = null;	// 输入write

		Set<String> fileNameSet = new HashSet<String>();

		String tfrPath = null;	// tfr.dat文件路径
	}

}
