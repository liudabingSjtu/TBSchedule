package com.taobao.pamirs.schedule;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 任务调度器，在TBScheduleManager的管理下实现多线程数据处理
 * @author xuannan
 *
 * @param <T>
 */
public class TBScheduleProcessorSleep<T> implements IScheduleProcessor,Runnable {
	
	private static transient Log logger = LogFactory.getLog(TBScheduleProcessorSleep.class);
	final  LockObject   m_lockObject = new LockObject();
	List<Thread> threadList =  Collections.synchronizedList(new ArrayList<Thread>());
	/**
	 * 任务管理器
	 */
	protected TBScheduleManager scheduleManager;
	/**
	 * 任务类型
	 */
	ScheduleTaskType taskTypeInfo;
	
	/**
	 * 任务处理的接口类
	 */
	protected IScheduleTaskDeal<T> taskDealBean;
		
	/**
	 * 当前任务队列的版本号
	 */
	protected long taskListVersion = 0;
	final Object lockVersionObject = new Object();
	final Object lockRunningList = new Object();

	//一个processor拥有一个可执行的任务队列
	protected List<T> taskList = Collections.synchronizedList(new ArrayList<T>());

	/**
	 * 是否可以批处理
	 */
	boolean isMutilTask = false;
	
	/**
	 * 是否已经获得终止调度信号
	 */
	boolean isStopSchedule = false;// 用户停止队列调度
	boolean isSleeping = false;
	
	StatisticsInfo statisticsInfo;
	/**
	 * 创建一个调度处理器 
	 * @param aManager
	 * @param aTaskDealBean
	 * @param aStatisticsInfo
	 * @throws Exception
	 */
	public TBScheduleProcessorSleep(TBScheduleManager aManager,
			IScheduleTaskDeal<T> aTaskDealBean,	StatisticsInfo aStatisticsInfo) throws Exception {
		this.scheduleManager = aManager;
		this.statisticsInfo = aStatisticsInfo;
		this.taskTypeInfo = this.scheduleManager.getTaskTypeInfo();
		this.taskDealBean = aTaskDealBean;
		if (this.taskDealBean instanceof IScheduleTaskDealSingle<?>) {
			if (taskTypeInfo.getExecuteNumber() > 1) {
				taskTypeInfo.setExecuteNumber(1);
			}
			isMutilTask = false;
		} else {
			isMutilTask = true;
		}
		if (taskTypeInfo.getFetchDataNumber() < taskTypeInfo.getThreadNumber() * 10) {
			logger.warn("参数设置不合理，系统性能不佳。【每次从数据库获取的数量fetchnum】 >= 【线程数量threadnum】 *【最少循环次数10】 ");
		}
		for (int i = 0; i < taskTypeInfo.getThreadNumber(); i++) {
			this.startThread(i);
		}
	}

	/**
	 * 需要注意的是，调度服务器从配置中心注销的工作，必须在所有线程退出的情况下才能做
	 * @throws Exception
	 */
	public void stopSchedule() throws Exception {
		// 设置停止调度的标志,调度线程发现这个标志，执行完当前任务后，就退出调度
		this.isStopSchedule = true;
		//清除所有未处理任务,但已经进入处理队列的，需要处理完毕
		this.taskList.clear();
	}

	private void startThread(int index) {
		Thread thread = new Thread(this);
		threadList.add(thread);
		String threadName = this.scheduleManager.getScheduleServer().getTaskType()+"-" 
				+ this.scheduleManager.getCurrentSerialNumber() + "-exe"
				+ index;
		thread.setName(threadName);
		thread.start();
	}


	   //按顺序取得单个任务或者多个任务
	   public synchronized Object getScheduleTaskId() {
		     if (this.taskList.size() > 0)
		    	 return this.taskList.remove(0);  // 按正序处理
		     return null;
		   }

		   public synchronized Object[] getScheduleTaskIdMulti() {
		       if (this.taskList.size() == 0){
		         return null;
		       }
		       int size = taskList.size() > taskTypeInfo.getExecuteNumber() ? taskTypeInfo.getExecuteNumber()
						: taskList.size();
		       Object[] result = new Object[size];
		       for(int i=0;i<size;i++){
		      	 result[i] = this.taskList.remove(0);  // 按正序处理
		       }
		       return result;
		   }

	public void clearAllHasFetchData() {
		this.taskList.clear();
	}
	public boolean isDealFinishAllData() {
		return this.taskList.size() == 0 ;
	}
	
	public boolean isSleeping(){
    	return this.isSleeping;
    }
	protected int loadScheduleData() {
		try {
           //在每次数据处理完毕后休眠固定的时间
			if (this.taskTypeInfo.getSleepTimeInterval() > 0) {
				if(logger.isTraceEnabled()){
					logger.trace("处理完一批数据后休眠：" + this.taskTypeInfo.getSleepTimeInterval());
				}
				this.isSleeping = true;
			    Thread.sleep(taskTypeInfo.getSleepTimeInterval());
			    this.isSleeping = false;
			    
				if(logger.isTraceEnabled()){
					logger.trace("处理完一批数据后休眠后恢复");
				}
			}
			
			List<TaskItemDefine> taskItems = this.scheduleManager.getCurrentScheduleTaskItemList();
			// 根据队列信息查询需要调度的数据，然后增加到任务列表中
			if (taskItems.size() > 0) {
				//交给处理类bean判断哪个任务是需要处理的
				List<T> tmpList = this.taskDealBean.selectTasks(
						taskTypeInfo.getTaskParameter(),
						scheduleManager.getScheduleServer().getOwnSign(),
						this.scheduleManager.getTaskItemCount(), taskItems,
						taskTypeInfo.getFetchDataNumber());
				scheduleManager.getScheduleServer().setLastFetchDataTime(new Timestamp(ScheduleUtil.getCurrentTimeMillis()));
				if(tmpList != null){
				   this.taskList.addAll(tmpList);
				}
			} else {
				if(logger.isTraceEnabled()){
					   logger.trace("没有获取到需要处理的数据队列");
				}
			}
			addFetchNum(taskList.size(),"TBScheduleProcessor.loadScheduleData");
			return this.taskList.size();
		} catch (Throwable ex) {
			logger.error("Get tasks error.", ex);
		}
		return 0;
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "static-access" })
	public void run(){
	      try {
	        long startTime =0;
	        while(true){
	          this.m_lockObject.addThread();
	          Object executeTask;
	          while (true) {
	            if(this.isStopSchedule == true){//停止队列调度
	              this.m_lockObject.realseThread();
	              this.m_lockObject.notifyOtherThread();//通知所有的休眠线程
				  this.threadList.remove(Thread.currentThread());
				  if(this.threadList.size()==0){
						this.scheduleManager.unRegisterScheduleServer();
				  }

				  return;
	            }
	            
	            //加载调度任务
	            if(this.isMutilTask == false){
					//取一个任务
	              executeTask = this.getScheduleTaskId();
	            }else{
					//按照配置取一定数量的任务
	              executeTask = this.getScheduleTaskIdMulti();
	            }
	            //队列中的任务执行完成后跳出循环，不论成功失败
	            if(executeTask == null){
	              break;
	            }
	            
	            try {
	            //运行相关的程序
	              startTime =ScheduleUtil.getCurrentTimeMillis();
	              if (this.isMutilTask == false) {
					  //执行继承IScheduleTaskDealSingle对象中excute方法
					  //任务的执行方式为成功失败 通过bolean返回
						if (((IScheduleTaskDealSingle) this.taskDealBean).execute(executeTask,scheduleManager.getScheduleServer().getOwnSign()) == true) {
							//统计任务运行时间，和成功次数
							addSuccessNum(1, ScheduleUtil.getCurrentTimeMillis()
									- startTime,
									"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
						} else {
							addFailNum(1, ScheduleUtil.getCurrentTimeMillis()
									- startTime,
									"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
						}
					} else {
						if (((IScheduleTaskDealMulti) this.taskDealBean)
								.execute((Object[]) executeTask,scheduleManager.getScheduleServer().getOwnSign()) == true) {
							addSuccessNum(((Object[]) executeTask).length, ScheduleUtil
									.getCurrentTimeMillis()
									- startTime,
									"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
						} else {
							addFailNum(((Object[]) executeTask).length, ScheduleUtil
									.getCurrentTimeMillis()
									- startTime,
									"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
						}
					} 
	            }catch (Throwable ex) {
					if (this.isMutilTask == false) {
						addFailNum(1, ScheduleUtil.getCurrentTimeMillis() - startTime,
								"TBScheduleProcessor.run");
					} else {
						addFailNum(((Object[]) executeTask).length, ScheduleUtil
								.getCurrentTimeMillis()
								- startTime,
								"TBScheduleProcessor.run");
					}
					logger.warn("Task :" + executeTask + " 处理失败", ex);				
	            }
	          }
	          //当前队列中所有的任务都已经完成了。
	            if(logger.isTraceEnabled()){
				   logger.trace(Thread.currentThread().getName() +"：当前运行线程数量:" +this.m_lockObject.count());
			    }
				//是否可以休眠（最后一个线程不能休眠）
				//使用m_lockObject休眠
				if (this.m_lockObject.realseThreadButNotLast() == false)
				//我是最后一个线程了
				{
					int size = 0;
					Thread.currentThread().sleep(100);
					//睡一会
					startTime = ScheduleUtil.getCurrentTimeMillis();
					// //在每次数据处理完毕后休眠固定的时间，并重新加载任务
					size = this.loadScheduleData();
					if (size > 0) {
						//取到任务，唤醒其他全部线程，开始抢任务
						this.m_lockObject.notifyOtherThread();
					} else {
						//没有取到任务，准备睡眠
						//判断当没有数据的是否，是否需要退出调度
						if (this.isStopSchedule == false && this.scheduleManager.isContinueWhenData()== true ){						 
							if(logger.isTraceEnabled()){
								   logger.trace("没有装载到数据，start sleep");
							}
							this.isSleeping = true;
							//睡眠
						    Thread.currentThread().sleep(this.scheduleManager.getTaskTypeInfo().getSleepTimeNoData());
						    this.isSleeping = false;
						    
						    if(logger.isTraceEnabled()){
								   logger.trace("Sleep end");
							}
						}else{
							//没有数据，退出调度，唤醒所有沉睡线程，其他线程被唤醒后，也将被推出，
							this.m_lockObject.notifyOtherThread();
						}
					}
					this.m_lockObject.realseThread();
				} else {// 将当前线程放置到等待队列中。直到有线程装载到了新的任务数据
					if(logger.isTraceEnabled()){
						   logger.trace("不是最后一个线程，sleep");
					}
					//可以被其他thread通过notifyAll唤醒
					this.m_lockObject.waitCurrentThread();
				}
	        }
	      }
	      catch (Throwable e) {
	    	  logger.error(e.getMessage(), e);
	      }
	    }

	public void addFetchNum(long num, String addr) {
		
        this.statisticsInfo.addFetchDataCount(1);
        this.statisticsInfo.addFetchDataNum(num);
	}

	public void addSuccessNum(long num, long spendTime, String addr) {
        this.statisticsInfo.addDealDataSucess(num);
        this.statisticsInfo.addDealSpendTime(spendTime);
	}

	public void addFailNum(long num, long spendTime, String addr) {
      this.statisticsInfo.addDealDataFail(num);
      this.statisticsInfo.addDealSpendTime(spendTime);
	}
}
