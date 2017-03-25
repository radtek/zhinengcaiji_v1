package framework.ui;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.LayoutManager;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import alarm.AlarmMgr;
import console.ConsoleMgr;
import datalog.DataLogMgr;
import framework.DataLifecycleMgr;
import framework.IGP;
import framework.ScanThread;
import framework.SystemConfig;

/**
 * <p>
 * IGP的窗口界面，以SWING方式呈现。
 * <p>
 * 
 * @author ChenSijiang 2010-11-8
 * @since 1.2
 */
public class IGPWindow extends BaseWindow {

	private static final long serialVersionUID = 3840461744414857260L;

	private static final String STR_START = "启动";

	private static final String STR_STOP = "停止";

	private static final String STR_TITLE = "IGP " + SystemConfig.getInstance().getEdition() + " - ";

	private JButton btnStart = new JButton(STR_START);

	private JButton btnConfig = new JButton("IGP配置");

	private JPanel contentPanel = null;

	private LayoutManager windowLayout = new GridLayout(1, 1);

	// 采集是否正在运行
	private boolean isRunning = false;

	/**
	 * <p>
	 * 构造方法。
	 * </p>
	 * 
	 * @throws Exception
	 *             抛出所有异常
	 */
	public IGPWindow() throws Exception {
		super();
		toStoppedStatus();
		contentPanel = (JPanel) getContentPane();
		contentPanel.setLayout(windowLayout);
		initLayout();
		initEvent();
		setResizable(false);
		setVisible(true);
		setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
	}

	/**
	 * 布局
	 */
	private void initLayout() {
		contentPanel.add(btnStart);
		// contentPanel.add(btnConfig);
	}

	/**
	 * 初始化事件
	 */
	private void initEvent() {
		btnStart.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				if (isRunning) {
					int taskCount = TaskMgr.getInstance().size();
					String msg = null;
					if (taskCount > 0) {
						msg = "当前有" + taskCount + "个任务正在运行，您确定要停止采集吗？";
					} else {
						msg = "您确定要停止采集吗？";
					}
					int result = JOptionPane.showConfirmDialog(IGPWindow.this, msg, "停止采集", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE);
					if (result == JOptionPane.YES_OPTION) {
						Thread th = new Thread(new Runnable() {

							@Override
							public void run() {
								StoppingDialog dlg = new StoppingDialog();
								dlg.setVisible(true);
								toStoppedStatus();
							}
						});
						th.start();
					}
				} else {
					new Thread(new Runnable() {

						@Override
						public void run() {
							IGP.runIGP();
						}
					}).start();
					toRunningStatus();
				}
			}
		});

		addWindowListener(new WindowAdapter() {

			@Override
			public void windowClosing(WindowEvent e) {
				super.windowClosing(e);
				if (isRunning) {
					JOptionPane.showMessageDialog(IGPWindow.this, "采集正在运行，请点击“停止”按钮来结束程序。", "提示", JOptionPane.WARNING_MESSAGE);
				} else {
					dispose();
					System.exit(0);
				}
			}

		});

		btnConfig.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				try {
					new ConfigWindow(IGPWindow.this).setVisible(true);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		});
	}

	/**
	 * 将窗口转为IGP运行中状态。
	 */
	private void toRunningStatus() {
		btnStart.setText(STR_STOP);
		setTitle(STR_TITLE + "运行中");
		isRunning = true;
	}

	/**
	 * 将窗口转为IGP停止状态。
	 */
	private void toStoppedStatus() {
		btnStart.setText(STR_START);
		setTitle(STR_TITLE + "已停止");
		isRunning = false;
	}

	/**
	 * 自动启动IGP
	 */
	@SuppressWarnings("unused")
	private void autoStart() {
		JDialog dlg = new ModalDialog(this);
		dlg.setTitle("自动启动采集");
		dlg.setSize(getWidth() / 2, getHeight() / 2);
		dlg.setModal(true);
		dlg.setVisible(true);
	}

	private class StoppingDialog extends ModalDialog {

		private static final long serialVersionUID = -6267828376634094961L;

		private JTextArea text = new JTextArea();

		private JScrollPane pane = new JScrollPane(text);

		public StoppingDialog() {
			super(IGPWindow.this);
			setTitle("正在停止采集");
			setLayout(new BorderLayout());
			add(pane, BorderLayout.CENTER);
			text.setEditable(false);
			setSize((int) (parent.getWidth() * 1.2), (int) (parent.getHeight() * 2));
			setDefaultCloseOperation(StoppingDialog.DO_NOTHING_ON_CLOSE);
			addWindowListener(new WindowAdapter() {

				@Override
				public void windowOpened(WindowEvent e) {
					super.windowOpened(e);
					new Thread(new Runnable() {

						@Override
						public void run() {
							ScanThread scanThread = ScanThread.getInstance();
							scanThread.setEndAction(new ScanThread.ScanEndAction() {

								@Override
								public void actionPerformed(TaskMgr taskMgr) {
									List<CollectObjInfo> tasks = taskMgr.list();
									for (CollectObjInfo task : tasks) {
										String taskType = (task instanceof RegatherObjInfo ? "补采任务" : "采集任务");
										Thread th = task.getCollectThread();
										long id = task.getKeyID();
										print("正在等待" + taskType + "(id:" + id + ")结束");
										try {
											th.join();
										} catch (InterruptedException ex) {
											ex.printStackTrace();
										}
										th.interrupt();
										println("......已结束");
									}
									// 结束告警,生命周期线程，连接池
									DataLifecycleMgr.getInstance().stop();
									AlarmMgr.getInstance().shutdown();
									println("正在提交数据库日志...");
									DataLogMgr.getInstance().commit(); // 提交尚未入库的数据库日志
									println("数据库日志提交完毕...");
									println("正在释放数据库连接...");
									LogMgr.getInstance().getDBLogger().dispose();
									CommonDB.closeDbConnection();
									DbPool.close();
									println("数据库连接释放完毕...");

									// 结束控制台线程
									ConsoleMgr.getInstance().stop();
									println("数据库连接释放完毕...");
									try {
										for (int i = 1; i <= 3; i++) {
											println("IGP将在" + (4 - i) + "秒后关闭...");
											Thread.sleep(1000);
										}
									} catch (InterruptedException ex) {
										ex.printStackTrace();
									}
									System.exit(0);
								}

							});
							scanThread.stopScan();
						}
					}).start();
				}

			});

		}

		private void print(Object str) {
			text.append(str == null ? "" : str.toString());
		}

		private void println(Object str) {
			print(str);
			print("\n");
		}
	}

	public static void main(String[] args) {
		try {
			new IGPWindow();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
