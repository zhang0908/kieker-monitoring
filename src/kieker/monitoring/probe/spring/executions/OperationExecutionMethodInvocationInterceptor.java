/***************************************************************************
 * Copyright 2017 Kieker Project (http://kieker-monitoring.net)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

package kieker.monitoring.probe.spring.executions;

import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxy;
import org.springframework.aop.framework.ReflectiveMethodInvocation;

import kieker.common.record.controlflow.OperationExecutionRecord;
import kieker.monitoring.core.controller.IMonitoringController;
import kieker.monitoring.core.controller.MonitoringController;
import kieker.monitoring.core.registry.ControlFlowRegistry;
import kieker.monitoring.core.registry.SessionRegistry;
import kieker.monitoring.probe.IMonitoringProbe;
import kieker.monitoring.timer.ITimeSource;

/**
 * @author Marco Luebcke, Andre van Hoorn, Jan Waller
 *
 * @since 0.91
 */
public class OperationExecutionMethodInvocationInterceptor implements MethodInterceptor, IMonitoringProbe {
	private static final Logger LOGGER = LoggerFactory.getLogger(OperationExecutionMethodInvocationInterceptor.class);

	private static final SessionRegistry SESSION_REGISTRY = SessionRegistry.INSTANCE;
	private static final ControlFlowRegistry CF_REGISTRY = ControlFlowRegistry.INSTANCE;
	
	private final IMonitoringController monitoringCtrl;
	private final ITimeSource timeSource;
	private final String hostname;
	
	private OperationExecutionMethodInvocationDaoBuilder daoBuilderBean;
	
	private static final String NODE_TYPE_CLASS_FUNCTION = "CLASS-FUNCTION";
	private static final String NODE_TYPE_DATABASE_SQL = "DATABASE-SQL";
	private static final String PERSISTENT_TYPE_MYBATIS = "MyBatisDao";

	public OperationExecutionMethodInvocationInterceptor() {
		this(MonitoringController.getInstance());
	}

	/**
	 * This constructor is mainly used for testing, providing a custom {@link IMonitoringController} instead of using the singleton instance.
	 *
	 * @param monitoringController
	 *            must not be null
	 */
	public OperationExecutionMethodInvocationInterceptor(final IMonitoringController monitoringController) {
		this.monitoringCtrl = monitoringController;
		this.timeSource = this.monitoringCtrl.getTimeSource();
		this.hostname = this.monitoringCtrl.getHostname();
	}

	/**
	 * @see org.aopalliance.intercept.MethodInterceptor#invoke(org.aopalliance.intercept.MethodInvocation)
	 */
	@Override
	public Object invoke(final MethodInvocation invocation) throws Throwable { // NOCS (IllegalThrowsCheck)
		
		if (!this.monitoringCtrl.isMonitoringEnabled()) {
			return invocation.proceed();
		}
		final String signature = invocation.getMethod().toString();
		if (!this.monitoringCtrl.isProbeActivated(signature)) {
			return invocation.proceed();
		}
		
		String previousSignature = CF_REGISTRY.getLocalThreadSignature();
		if (signature != null && previousSignature != null && previousSignature.equalsIgnoreCase(signature)) {
			return invocation.proceed();
		} else {
			CF_REGISTRY.setLocalThreadSignature(signature);
		}

		final String sessionId = SESSION_REGISTRY.recallThreadLocalSessionId();
		final int eoi; // this is executionOrderIndex-th execution in this trace
		final int ess; // this is the height in the dynamic call tree of this execution
		final boolean entrypoint;
		long traceId = CF_REGISTRY.recallThreadLocalTraceId(); // traceId, -1 if entry point
		if (traceId == -1) {
			entrypoint = true;
			traceId = CF_REGISTRY.getAndStoreUniqueThreadLocalTraceId();
			CF_REGISTRY.storeThreadLocalEOI(0);
			CF_REGISTRY.storeThreadLocalESS(1); // next operation is ess + 1
			eoi = 0;
			ess = 0;
		} else {
			entrypoint = false;
			eoi = CF_REGISTRY.incrementAndRecallThreadLocalEOI(); // ess > 1
			ess = CF_REGISTRY.recallAndIncrementThreadLocalESS(); // ess >= 0
			if ((eoi == -1) || (ess == -1)) {
				LOGGER.error("eoi and/or ess have invalid values: eoi == {} ess == {}", eoi, ess);
				this.monitoringCtrl.terminateMonitoring();
			}
		}
		final long tin = this.timeSource.getTime();
		final Object retval;
		try {
			retval = invocation.proceed();
		} finally {
			final long tout = this.timeSource.getTime();
			
			String lable = signature;
			
			try {
				
				if (signature.toLowerCase().indexOf("cruddao") != -1) {
					
					lable = (((ReflectiveMethodInvocation)invocation).targetClass).getInterfaces()[0].getName() + "." + invocation.getMethod().getName();
					
				}
				
			} catch (Exception e) {
				
				lable = signature;
				
				e.printStackTrace();
				
			}
			
			this.monitoringCtrl.newMonitoringRecord(
					new OperationExecutionRecord(lable, sessionId, traceId, tin, tout, NODE_TYPE_CLASS_FUNCTION, eoi, ess));
			
			// record sql information: sql id, tableName.
			recordSQLInfo4DaoInstance(invocation, ess + 1);
			
			// cleanup
			if (entrypoint) {
				CF_REGISTRY.unsetThreadLocalTraceId();
				CF_REGISTRY.unsetThreadLocalEOI();
				CF_REGISTRY.unsetThreadLocalESS();
			} else {
				CF_REGISTRY.storeThreadLocalESS(ess); // next operation is ess
			}
			
		}
		return retval;
	}
	
	private void recordSQLInfo4DaoInstance(final MethodInvocation invocation, int parentNodeIndex) {
		
		try
		{
			
			if (isPersistentClassMethod(invocation)) {
				
				Map<String, List<String>> tableInfoMap = daoBuilderBean.parsePersistentMethodSqlInfo(invocation);
				
				Iterator<String> tableNameIterator = tableInfoMap.keySet().iterator();
				
				while (tableNameIterator.hasNext()) {
					
					String sqlId = tableNameIterator.next();
					
//					recordSQLTableInfo(NODE_TYPE_DATABASE_SQL, sqlId, parentNodeIndex);
					
					List<String> tableNameList = tableInfoMap.get(sqlId);
					
					int tableParentSqlIndex = parentNodeIndex;// + 1;
					
					for (int i = 0; i < tableNameList.size(); i++) {
						
						recordSQLTableInfo(NODE_TYPE_DATABASE_SQL, tableNameList.get(i), tableParentSqlIndex);
						
//						for (int j = i; j < tableNameList.size(); j++) {
//							
//							recordSQLTableInfo(NODE_TYPE_DATABASE_SQL, tableNameList.get(j), tableParentSqlIndex + 1);
//							
//						}
					}
					
				}
				
			}
		} catch (Exception e) {
			
			LOGGER.warn(e.getMessage());
			
		}
		
	}
	
	private void recordSQLTableInfo(String nodeType, final String tableName, final int parentNodeIndex) {
		
		final String sessionId = SESSION_REGISTRY.recallThreadLocalSessionId();
		final int eoi; // this is executionOrderIndex-th execution in this trace
		final int ess; // this is the height in the dynamic call tree of this execution
		long traceId = CF_REGISTRY.recallThreadLocalTraceId(); // traceId, -1 if entry point
		if (traceId == -1) {
			traceId = CF_REGISTRY.getAndStoreUniqueThreadLocalTraceId();
			CF_REGISTRY.storeThreadLocalEOI(0);
			CF_REGISTRY.storeThreadLocalESS(1); // next operation is ess + 1
			eoi = 0;
			ess = 0;
		} else {
			eoi = CF_REGISTRY.incrementAndRecallThreadLocalEOI(); // ess > 1
			ess = parentNodeIndex;//CF_REGISTRY.recallAndIncrementThreadLocalESS(); // ess >= 0
			if ((eoi == -1) || (ess == -1)) {
				LOGGER.error("eoi and/or ess have invalid values: eoi == {} ess == {}", eoi, ess);
				this.monitoringCtrl.terminateMonitoring();
			}
		}
		final long tin = this.timeSource.getTime();
		final long tout = tin;//this.timeSource.getTime();
		
		this.monitoringCtrl.newMonitoringRecord(
				new OperationExecutionRecord(tableName, sessionId, traceId, tin, tout, nodeType, eoi, ess));
		
	}
	
	private boolean isPersistentClassMethod(final MethodInvocation invocation) {
		
		Annotation[] annotations4Class = (((ReflectiveMethodInvocation)invocation).targetClass).getInterfaces()[0].getAnnotations();
		
		for (Annotation anno : annotations4Class) {
			
			if (PERSISTENT_TYPE_MYBATIS.equalsIgnoreCase(anno.annotationType().getSimpleName())) {
				
				return true;
				
			}
			
		}
		
		return false;
		
	}
	
	public void setDaoBuilderBean(OperationExecutionMethodInvocationDaoBuilder daoBuilderBean) {
		this.daoBuilderBean = daoBuilderBean;
	}

}
