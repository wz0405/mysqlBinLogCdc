package com.sync.demo;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class ServiceManager implements ApplicationContextAware {

	private static ApplicationContext ctx;
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		ServiceManager.ctx = applicationContext;
	}
	
	public static ApplicationContext getApplicationContext() throws Exception {
		return ctx;
	}
	

}
