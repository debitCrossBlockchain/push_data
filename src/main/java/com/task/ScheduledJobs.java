package com.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
public class ScheduledJobs {
	
	private static final Logger logger = LoggerFactory.getLogger(ScheduledJobs.class);
	
    @Scheduled(cron="0 0/1 8-20 * * ?") 
    public void executeblockchainTask() {
    	   logger.debug("開始執行抓取hive數據並處理！");
    	   try {
            //com.app.App.init();	  
    	   }catch(Exception ex) {
    		   logger.error(ex.getMessage()); 
    	   }
    }
       
    @Scheduled(cron="0 0/1 8-20 * * ?") 
    public void executeUploadTask() {
    	
    	// 间隔1分钟,执行工单上传任务	   	       
    	Thread current = Thread.currentThread();  
    	System.out.println("定时任务2:"+current.getId());
    	logger.info("ScheduledTest.executeUploadTask 定时任务2:"+current.getId() + ",name:"+current.getName());
    }
    
    @Scheduled(cron="0 0/3 5-23 * * ?") 
    public void executeUploadBackTask() {
    	
    	// 间隔3分钟,执行工单上传任务	   	   	    	       
    	Thread current = Thread.currentThread();  
    	System.out.println("定时任务3:"+current.getId());
    	logger.info("ScheduledTest.executeUploadBackTask 定时任务3:"+current.getId()+ ",name:"+current.getName());
    }    
      
}
