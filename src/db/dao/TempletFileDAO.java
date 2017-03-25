package db.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import util.Util;
import db.pojo.Templet;
import db.pojo.TempletFile;
import framework.SystemConfig;

/**
 * 模板文件操作类
 * 
 * @author YangJian
 * @since 1.0
 * @see TempletFile
 * @see Templet
 */
public class TempletFileDAO extends AbstractDAO<TempletFile> {

	/** 系统模板文件夹路径 */
	private String TempletFolderPath = SystemConfig.getInstance().getTempletPath();

	/** 当我们加载文件内容给前台用户时，如果超过了这个大小则不予以加载，建议用户直接下载文件到本地直接操作后再上传到服务器,默认为500KB */
	public static final long MAX_ALLOW_FILE_SIZE = 500 * 1000;

	/**
	 * 条件查询,对于文件内容不予以加载
	 */
	@Override
	public List<TempletFile> criteriaQuery(TempletFile tmpFile) {
		String name = null;
		boolean queryFlag = false;
		if (tmpFile != null) {
			name = tmpFile.getName();
			if (Util.isNotNull(name))
				queryFlag = true;
		}

		File folder = new File(TempletFolderPath);
		if (!folder.exists() || folder.isFile())
			return null;

		List<TempletFile> tFiles = new ArrayList<TempletFile>();

		File[] files = folder.listFiles();
		for (File f : files) {
			if (f.isDirectory())
				continue;

			TempletFile e = new TempletFile();
			if (queryFlag) {
				String fName = f.getName();
				// 符合查询的文件名
				if (fName.indexOf(name) > -1) {
					e.setName(fName);
					e.setSize(f.length());
					e.setModifyDate(Util.getDateString(new Date(f.lastModified())));
					tFiles.add(e);
				} else
					continue;
			} else {
				e.setName(f.getName());
				e.setSize(f.length());
				e.setModifyDate(Util.getDateString(new Date(f.lastModified())));
				tFiles.add(e);
			}
		}

		return tFiles;
	}

	/**
	 * 删除模板文件
	 */
	@Override
	public boolean delete(TempletFile entity) {
		if (entity == null || Util.isNull(entity.getName())) {
			return false;
		} else {
			String name = entity.getName();
			File f = new File(TempletFolderPath + File.separator + name);
			if (f.exists() && f.isFile()) {
				return f.delete();
			} else
				return false;
		}
	}

	/**
	 * 判断文件是否存在
	 */
	@Override
	public boolean exists(TempletFile entity) {
		if (entity == null || Util.isNull(entity.getName()))
			return false;

		String name = entity.getName();
		File f = new File(TempletFolderPath + File.separator + name);

		return f.exists();
	}

	/**
	 * 根据文件名获取模板文件信息
	 */
	@Override
	public TempletFile getByName(String name) {
		if (Util.isNull(name))
			return null;

		String fPath = TempletFolderPath + File.separator + name;
		File f = new File(fPath);
		if (f.exists() && f.isFile()) {
			TempletFile e = new TempletFile();
			e.setName(f.getName());
			e.setSize(f.length());
			e.setModifyDate(Util.getDateString(new Date(f.lastModified())));
			if (f.length() <= MAX_ALLOW_FILE_SIZE)
				e.setContent(this.getContent(fPath));
			else
				e.setContent("文件内容超过指定最大值(" + MAX_ALLOW_FILE_SIZE + " Byte),建议把文件下载本地进行编辑,之后再上传到采集机上!");

			return e;
		} else
			return null;
	}

	/**
	 * 保存文件内容
	 */
	@Override
	public boolean update(TempletFile entity) {
		String fileName = entity.getName();
		String content = entity.getContent();

		if (Util.isNull(fileName))
			return false;

		if (content == null)
			content = "";

		boolean ret = true;
		String fPath = TempletFolderPath + File.separator + fileName;
		OutputStream fout = null;
		try {
			fout = new FileOutputStream(fPath);
			byte[] b = content.getBytes();
			fout.write(b, 0, b.length);
			fout.flush();
		} catch (FileNotFoundException e) {
			ret = false;
		} catch (IOException e) {
			ret = false;
		} finally {
			if (fout != null) {
				try {
					fout.close();
				} catch (IOException e) {
				}
			}
		}

		return ret;
	}

	/**
	 * 读取指定文件的内容
	 * 
	 * @param fileName
	 *            文件的绝对路径
	 * @return
	 */
	private String getContent(String fileName) {
		if (Util.isNull(fileName))
			return null;

		StringBuffer content = new StringBuffer();
		BufferedReader br = null;
		try {
			String strLine = null;
			br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fileName))));

			while ((strLine = br.readLine()) != null) {
				content.append(strLine).append(System.getProperty("line.separator"));
			}
		} catch (FileNotFoundException e) {
			content.append("文件不存在,详细:").append(e.getMessage());
		} catch (IOException e) {
			content.append("文件访问出错,详细:").append(e.getMessage());
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}

		return content.toString();
	}

	/**
	 * 是否超过最大允许编辑时的文件大小
	 * 
	 * @param fileName
	 *            模板文件名，不包含路径
	 * @return
	 */
	public boolean isExceedMaxValue(String fileName) {
		if (Util.isNull(fileName))
			return false;

		String fPath = TempletFolderPath + File.separator + fileName;
		File f = new File(fPath);
		if (f.exists() && f.isFile()) {
			if (f.length() <= MAX_ALLOW_FILE_SIZE)
				return false;
			else
				return true;
		} else
			return false;
	}

	/**
	 * 给文件重命名
	 * 
	 * @param oldFileName
	 *            旧文件名
	 * @param newFileName
	 *            新文件名
	 * @return 如果新文件名已经存在则返回失败
	 */
	public boolean rename(String oldFileName, String newFileName) {
		if (Util.isNull(oldFileName) || Util.isNull(newFileName))
			return false;

		String fOldPath = TempletFolderPath + File.separator + oldFileName;
		File fOld = new File(fOldPath);
		if (!fOld.exists() || !fOld.isFile())
			return false;

		String fNewPath = TempletFolderPath + File.separator + newFileName;
		File fNew = new File(fNewPath);
		// 如果目标文件名存在则重命名失败
		if (fNew.exists() && fNew.isFile())
			return false;

		return fOld.renameTo(fNew);
	}

	/**
	 * 新建模板文件
	 * 
	 * @param newFileName
	 * @return
	 */
	public boolean newFile(String newFileName) {
		if (Util.isNull(newFileName))
			return false;

		String fNewPath = TempletFolderPath + File.separator + newFileName;
		File fNew = new File(fNewPath);
		// 如果目标文件名存在则新建失败
		if (fNew.exists() && fNew.isFile())
			return false;

		boolean b = true;
		try {
			b = fNew.createNewFile();
		} catch (IOException e) {
			b = false;
		}

		return b;
	}

	/**
	 * 获取系统模板文件夹位置
	 * 
	 * @return
	 */
	public String getTempletFolderPath() {
		return TempletFolderPath;
	}
}
