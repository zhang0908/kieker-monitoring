package kieker.monitoring.probe.spring.executions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxy;
import org.springframework.aop.framework.ReflectiveMethodInvocation;

public class OperationExecutionMethodInvocationDaoBuilder {
	
	private static final Logger logger = LoggerFactory.getLogger(OperationExecutionMethodInvocationDaoBuilder.class);
	
	private DefaultSqlSessionFactory sqlSessionFactoryBean;
	
	private ThreadLocal<String> dbName = new ThreadLocal<String>();
	
	public Map<String, List<String>> parsePersistentMethodSqlInfo(final MethodInvocation invocation) {
		
		Map<String, List<String>> tableInfoMap = new HashMap<String, List<String>>();
		
		try {
			
			String id = (((ReflectiveMethodInvocation)invocation).targetClass).getInterfaces()[0].getName() + "." + invocation.getMethod().getName();
			
			Configuration conf4DaoXml = sqlSessionFactoryBean.getConfiguration();
			
			Object parameter = invocation.getArguments() != null && invocation.getArguments().length > 0 ? invocation.getArguments()[0] : null;
			
			String sql = conf4DaoXml.getMappedStatement(id).getSqlSource().getBoundSql(parameter).getSql();
			
			String sqlId = assembleSqlId(conf4DaoXml.getMappedStatement(id).getResource(), 
					conf4DaoXml.getMappedStatement(id).getId());
			
			tableInfoMap.put(sqlId, parseTableNamesFromSql(sql));
			
		} catch (Exception e) {
			
			logger.warn(e.getMessage());
			
		}
		
		return tableInfoMap;
		
	}
	
	private List<String> parseTableNamesFromSql(String sql) throws SQLException {
		
		List<String> tableList = new ArrayList<String>();
		
		String dataBaseName = getDataBaseName();
		
		sql = sql.toLowerCase().replaceAll("\n", " ");
		
		String[] regexs = {"from\\s+(.*)\\s+where?", "update\\s+(.*)\\s+set", "delete from\\s+(.*)\\s+where"};//, "insert into\\s+(.*)\\("};
		
		for (String regex : regexs) {
			
			Pattern pattern = Pattern.compile(regex);
			
			Matcher matcher = pattern.matcher(sql);
			
			while (matcher.find()) {
				
				String tname = matcher.group(1);
				
				String[] sps1 = tname.trim().split(",");
				
				for (String sp1 : sps1) {
					
					String[] sps2 = sp1.trim().split("join");
					
					for (String sp2 : sps2) {
						
						tableList.add(dataBaseName + "." + sp2.trim().split(" ")[0].toUpperCase());
						
					}
					
				}
				
			}
			
		}
		
		if (sql.startsWith("insert into ")) {
			
			tableList.add(sql.substring("insert into ".length(), sql.indexOf("(")));
		}
		
		return tableList;
		
	}
	
	public static void main(String[] agrs) throws SQLException {
		
		String sql = "SELECT * FROM sys_user, sys_user11 WHERE id = #{userId}";
		
		new OperationExecutionMethodInvocationDaoBuilder().parseTableNamesFromSql(sql);
		
	}
	
	private String getDataBaseName() throws SQLException {
		
		String databaseName = dbName.get();
		
		if (databaseName == null || databaseName.trim().length() == 0) {
			
			Connection conn = null;
			
			try {
				
				conn = sqlSessionFactoryBean.getConfiguration().getEnvironment().getDataSource().getConnection();
				
				databaseName = conn.getCatalog();
				
			} finally {
				
				if (conn != null) {
					
					conn.close();
					
				}
				
			}
			
		}
		
		return databaseName;
		
	}
	
	private String assembleSqlId(String sourceFile, String sqlIdinSourceFile) {
		
		StringBuffer sqlId = new StringBuffer();
		
		if (sourceFile != null && sourceFile.length() > 0 && sourceFile.endsWith(".xml]")) {
			
			sqlId.append(sourceFile.substring(sourceFile.lastIndexOf("\\") + 1, sourceFile.lastIndexOf(".")));
			
		} else {
			
			sqlId.append("Annotation");
			
		}
		
		if (sqlIdinSourceFile != null && sqlIdinSourceFile.length() > 0) {
			
			sqlId.append(sqlIdinSourceFile.substring(sqlIdinSourceFile.lastIndexOf(".")));
			
		}
		
		return sqlId.toString().trim();
		
	}
	
	public void setSqlSessionFactoryBean(DefaultSqlSessionFactory sqlSessionFactoryBean) {
		this.sqlSessionFactoryBean = sqlSessionFactoryBean;
	}

}
