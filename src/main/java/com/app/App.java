package com.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.servlet.MultipartConfigElement;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.MultipartConfigFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.hive.HiveProcess;


@ComponentScan
@EnableAutoConfiguration
@EnableScheduling
@Configuration
public class App {
	
	@Value("${url}")
	private static String url;
	@Value("${user}")
	private static String user;
	@Value("${pwd}")
	private static String pwd;
	@Value("${kafkaAddress}")
	private static String kafkaAddress;
	@Value("${partition}")
	private static int partition;
	@Value("${path}")
	private static String path;
	@Value("${server.port}")
	private int port;
	@Value("${server.sessionTimeout}")
	private int sessionTimeout;
	

	public static void main(String[] args) {
		
		SpringApplication.run(App.class, args);	
	}
	

	public static void init()throws Exception{  
		int page = 20000;
		int sum = HiveProcess.QueryCount(url, user, pwd);
		System.out.println("read sum complete!");
		System.out.println(sum);
		int num = sum/page;
		int lastPage = sum%page;
		for(int i=1;i<num;i++) {
			HiveProcess.ProccessQuery(url, user, pwd,i*page, page, kafkaAddress);
			System.out.println(i);
		}
		HiveProcess.ProccessQuery(url, user, pwd,num*page, lastPage, kafkaAddress);
		System.out.println("complete!");
    } 
	
	public static int getPartition() {
	        return  partition;
	}
	
	@Bean
	public EmbeddedServletContainerFactory servletContainer() {
	    TomcatEmbeddedServletContainerFactory factory = new TomcatEmbeddedServletContainerFactory();
	    factory.setPort(port);
	    factory.setSessionTimeout(sessionTimeout, TimeUnit.SECONDS);
	    return factory;
	}
	
	@Bean  
    public MultipartConfigElement multipartConfigElement() {  
        MultipartConfigFactory factory = new MultipartConfigFactory();  
        factory.setMaxFileSize("102400KB");  
        factory.setMaxRequestSize("102400KB");  
        return factory.createMultipartConfig();  
    }
	
}

@Configuration
@ImportResource("/spring/applicationContext.xml")
class XmlImportingConfiguration {
}
