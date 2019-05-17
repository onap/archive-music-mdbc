/*
 * ============LICENSE_START====================================================
 * org.onap.music.mdbc
 * =============================================================================
 * Copyright (C) 2018 AT&T Intellectual Property. All rights reserved.
 * =============================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *             http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END======================================================
 */
package org.onap.music.logging;

import static com.att.eelf.configuration.Configuration.MDC_ALERT_SEVERITY;
import static com.att.eelf.configuration.Configuration.MDC_INSTANCE_UUID;
import static com.att.eelf.configuration.Configuration.MDC_KEY_REQUEST_ID;
import static com.att.eelf.configuration.Configuration.MDC_SERVER_FQDN;
import static com.att.eelf.configuration.Configuration.MDC_SERVER_IP_ADDRESS;
import static com.att.eelf.configuration.Configuration.MDC_SERVICE_INSTANCE_ID;
import static com.att.eelf.configuration.Configuration.MDC_SERVICE_NAME;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.MDC;
import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import com.att.eelf.configuration.SLF4jWrapper;

public class EELFLoggerDelegate extends SLF4jWrapper implements EELFLogger {


	public static final EELFLogger errorLogger = EELFManager.getInstance().getErrorLogger();
	public static final EELFLogger applicationLogger = EELFManager.getInstance().getApplicationLogger();
	public static final EELFLogger auditLogger = EELFManager.getInstance().getAuditLogger();
	public static final EELFLogger metricsLogger = EELFManager.getInstance().getMetricsLogger();
	public static final EELFLogger debugLogger = EELFManager.getInstance().getDebugLogger();
	// DateTime Format according to the ECOMP Application Logging Guidelines.
		private static final SimpleDateFormat ecompLogDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		

	private String className;
	private static ConcurrentMap<String, EELFLoggerDelegate> classMap = new ConcurrentHashMap<String, EELFLoggerDelegate>();

	public EELFLoggerDelegate(final String className) {
		super(className);
		this.className = className;
	}

	/**
	 * Convenience method that gets a logger for the specified class.
	 * 
	 * @see #getLogger(String)
	 * 
	 * @param clazz
	 * @return Instance of EELFLoggerDelegate
	 */
	public static EELFLoggerDelegate getLogger(Class<?> clazz) {
		return getLogger(clazz.getName());
	}

	/**
	 * Gets a logger for the specified class name. If the logger does not already
	 * exist in the map, this creates a new logger.
	 * 
	 * @param className
	 *            If null or empty, uses EELFLoggerDelegate as the class name.
	 * @return Instance of EELFLoggerDelegate
	 */
	public static EELFLoggerDelegate getLogger(final String className) {
		String classNameNeverNull = className == null || "".equals(className) ? EELFLoggerDelegate.class.getName()
				: className;
		EELFLoggerDelegate delegate = classMap.get(classNameNeverNull);
		if (delegate == null) {
			delegate = new EELFLoggerDelegate(className);
			classMap.put(className, delegate);
		}
		return delegate;
	}

	/**
	 * Logs a message at the lowest level: trace.
	 * 
	 * @param logger
	 * @param msg
	 */
	public void trace(EELFLogger logger, String msg) {
		if (logger.isTraceEnabled()) {
			MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
			logger.trace(msg);
			MDC.remove(LoggerProperties.MDC_CLASS_NAME);
		}
	}

	/**
	 * Logs a message with parameters at the lowest level: trace.
	 * 
	 * @param logger
	 * @param msg
	 * @param arguments
	 */
	public void trace(EELFLogger logger, String msg, Object... arguments) {
		if (logger.isTraceEnabled()) {
			MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
			logger.trace(msg, arguments);
			MDC.remove(LoggerProperties.MDC_CLASS_NAME);
		}
	}

	/**
	 * Logs a message and throwable at the lowest level: trace.
	 * 
	 * @param logger
	 * @param msg
	 * @param th
	 */
	public void trace(EELFLogger logger, String msg, Throwable th) {
		if (logger.isTraceEnabled()) {
			MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
			logger.trace(msg, th);
			MDC.remove(LoggerProperties.MDC_CLASS_NAME);
		}
	}

	/**
	 * Logs a message at the second-lowest level: debug.
	 * 
	 * @param logger
	 * @param msg
	 */
	public void debug(EELFLogger logger, String msg) {
		if (logger.isDebugEnabled()) {
			MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
			logger.debug(msg);
			MDC.remove(LoggerProperties.MDC_CLASS_NAME);
		}
	}

	/**
	 * Logs a message with parameters at the second-lowest level: debug.
	 * 
	 * @param logger
	 * @param msg
	 * @param arguments
	 */
	public void debug(EELFLogger logger, String msg, Object... arguments) {
		if (logger.isDebugEnabled()) {
			MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
			logger.debug(msg, arguments);
			MDC.remove(LoggerProperties.MDC_CLASS_NAME);
		}
	}

	/**
	 * Logs a message and throwable at the second-lowest level: debug.
	 * 
	 * @param logger
	 * @param msg
	 * @param th
	 */
	public void debug(EELFLogger logger, String msg, Throwable th) {
		if (logger.isDebugEnabled()) {
			MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
			logger.debug(msg, th);
			MDC.remove(LoggerProperties.MDC_CLASS_NAME);
		}
	}

	/**
	 * Logs a message at info level.
	 * 
	 * @param logger
	 * @param msg
	 */
	public void info(EELFLogger logger, String msg) {
		MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
		logger.info(msg);
		MDC.remove(LoggerProperties.MDC_CLASS_NAME);
	}

	/**
	 * Logs a message with parameters at info level.
	 *
	 * @param logger
	 * @param msg
	 * @param arguments
	 */
	public void info(EELFLogger logger, String msg, Object... arguments) {
		MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
		logger.info(msg, arguments);
		MDC.remove(LoggerProperties.MDC_CLASS_NAME);
	}

	/**
	 * Logs a message and throwable at info level.
	 * 
	 * @param logger
	 * @param msg
	 * @param th
	 */
	public void info(EELFLogger logger, String msg, Throwable th) {
		MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
		logger.info(msg, th);
		MDC.remove(LoggerProperties.MDC_CLASS_NAME);
	}

	/**
	 * Logs a message at warn level.
	 * 
	 * @param logger
	 * @param msg
	 */
	public void warn(EELFLogger logger, String msg) {
		MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
		logger.warn(msg);
		MDC.remove(LoggerProperties.MDC_CLASS_NAME);
	}

	/**
	 * Logs a message with parameters at warn level.
	 * 
	 * @param logger
	 * @param msg
	 * @param arguments
	 */
	public void warn(EELFLogger logger, String msg, Object... arguments) {
		MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
		logger.warn(msg, arguments);
		MDC.remove(LoggerProperties.MDC_CLASS_NAME);
	}

	/**
	 * Logs a message and throwable at warn level.
	 * 
	 * @param logger
	 * @param msg
	 * @param th
	 */
	public void warn(EELFLogger logger, String msg, Throwable th) {
		MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
		logger.warn(msg, th);
		MDC.remove(LoggerProperties.MDC_CLASS_NAME);
	}

	/**
	 * Logs a message at error level.
	 * 
	 * @param logger
	 * @param msg
	 */
	public void error(EELFLogger logger, String msg) {
		MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
		logger.error(msg);
		MDC.remove(LoggerProperties.MDC_CLASS_NAME);
	}

	/**
	 * Logs a message with parameters at error level.
	 * 
	 * @param logger
	 * @param msg
	 * @param arguments
	 */
	public void error(EELFLogger logger, String msg, Object... arguments) {
		MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
		logger.warn(msg, arguments);
		MDC.remove(LoggerProperties.MDC_CLASS_NAME);
	}

	/**
	 * Logs a message and throwable at error level.
	 * 
	 * @param logger
	 * @param msg
	 * @param th
	 */
	public void error(EELFLogger logger, String msg, Throwable th) {
		MDC.put(LoggerProperties.MDC_CLASS_NAME, className);
		logger.warn(msg, th);
		MDC.remove(LoggerProperties.MDC_CLASS_NAME);
	}


	/**
	 * Initializes the logger context.
	 */
	public void init() {
		setGlobalLoggingContext();
		final String msg = "############################ Logging is started. ############################";
		// These loggers emit the current date-time without being told.
		info(applicationLogger, msg);
		error(errorLogger, msg);
		debug(debugLogger, msg);
		// Audit and metrics logger must be told start AND stop times
		final String currentDateTime = getCurrentDateTimeUTC();
		// Set the MDC with audit properties
		MDC.put(LoggerProperties.AUDITLOG_BEGIN_TIMESTAMP, currentDateTime);
		MDC.put(LoggerProperties.AUDITLOG_END_TIMESTAMP, currentDateTime);
		info(auditLogger, msg);
		MDC.remove(LoggerProperties.AUDITLOG_BEGIN_TIMESTAMP);
		MDC.remove(LoggerProperties.AUDITLOG_END_TIMESTAMP);
		// Set the MDC with metrics properties
		MDC.put(LoggerProperties.METRICSLOG_BEGIN_TIMESTAMP, currentDateTime);
		MDC.put(LoggerProperties.METRICSLOG_END_TIMESTAMP, currentDateTime);
		info(metricsLogger, msg);
		MDC.remove(LoggerProperties.METRICSLOG_BEGIN_TIMESTAMP);
		MDC.remove(LoggerProperties.METRICSLOG_END_TIMESTAMP);
	}
	
	
	public static String getCurrentDateTimeUTC() {
		String currentDateTime = ecompLogDateFormat.format(new Date());
		return currentDateTime;
	}


	
	/**
	 * Builds a message using a template string and the arguments.
	 * 
	 * @param message
	 * @param args
	 * @return
	 */
	private String formatMessage(String message, Object... args) {
		StringBuilder sbFormattedMessage = new StringBuilder();
		if (args != null && args.length > 0 && message != null && message != "") {
			MessageFormat mf = new MessageFormat(message);
			sbFormattedMessage.append(mf.format(args));
		} else {
			sbFormattedMessage.append(message);
		}

		return sbFormattedMessage.toString();
	}

	/**
	 * Loads all the default logging fields into the MDC context.
	 */
	private void setGlobalLoggingContext() {
		MDC.put(MDC_SERVICE_INSTANCE_ID, "");
		MDC.put(MDC_ALERT_SEVERITY, AlarmSeverityEnum.INFORMATIONAL.toString());
		try {
			MDC.put(MDC_SERVER_FQDN, InetAddress.getLocalHost().getHostName());
			MDC.put(MDC_SERVER_IP_ADDRESS, InetAddress.getLocalHost().getHostAddress());
			MDC.put(MDC_INSTANCE_UUID, LoggerProperties.getProperty(LoggerProperties.INSTANCE_UUID));
		} catch (Exception e) {
			errorLogger.error("setGlobalLoggingContext failed", e);
		}
	}

	public static void mdcPut(String key, String value) {
		MDC.put(key, value);
	}

	public static String mdcGet(String key) {
		return MDC.get(key);
	}

	public static void mdcRemove(String key) {
		MDC.remove(key);
	}

	/**
	 * Loads the RequestId/TransactionId into the MDC which it should be receiving
	 * with an each incoming REST API request. Also, configures few other request
	 * based logging fields into the MDC context.
	 * 
	 * @param req
	 * @param appName
	 */
	public void setRequestBasedDefaultsIntoGlobalLoggingContext(HttpServletRequest req, String appName,String reqId,String loginId) {// Load the default fields
		// Load the default fields
				setGlobalLoggingContext();

				// Load the request based fields
				if (req != null) {
					// Load the Request into MDC context.
					
					MDC.put(MDC_KEY_REQUEST_ID, reqId);

					// Load user agent into MDC context, if available.
					String accessingClient = req.getHeader(LoggerProperties.USERAGENT_NAME);
					if (accessingClient != null && !"".equals(accessingClient) && (accessingClient.contains("Mozilla")
							|| accessingClient.contains("Chrome") || accessingClient.contains("Safari"))) {
						accessingClient = appName + "_FE";
					}
					MDC.put(LoggerProperties.PARTNER_NAME, accessingClient);

					// Protocol, Rest URL & Rest Path
					MDC.put(LoggerProperties.FULL_URL, LoggerProperties.UNKNOWN);
					MDC.put(LoggerProperties.PROTOCOL, LoggerProperties.HTTP);
					String restURL = getFullURL(req);
					if (restURL != null && restURL != "") {
						MDC.put(LoggerProperties.FULL_URL, restURL);
						if (restURL.toLowerCase().contains("https")) {
							MDC.put(LoggerProperties.PROTOCOL, LoggerProperties.HTTPS);
						}
					}

					// Rest Path
					MDC.put(MDC_SERVICE_NAME, req.getServletPath());

					// Client IPAddress i.e. IPAddress of the remote host who is making
					// this request.
					String clientIPAddress = req.getHeader("X-FORWARDED-FOR");
					if (clientIPAddress == null) {
						clientIPAddress = req.getRemoteAddr();
					}
					MDC.put(LoggerProperties.CLIENT_IP_ADDRESS, clientIPAddress);

					// Load loginId into MDC context.
					MDC.put(LoggerProperties.MDC_LOGIN_ID, "Unknown");

				

					if (loginId != null && loginId != "") {
						MDC.put(LoggerProperties.MDC_LOGIN_ID, loginId);
					}
				}
	}
	

	
	
	public static String getFullURL(HttpServletRequest request) {
		if (request != null) {
			StringBuffer requestURL = request.getRequestURL();
			String queryString = request.getQueryString();

			if (queryString == null) {
				return requestURL.toString();
			} else {
				return requestURL.append('?').append(queryString).toString();
			}
		}
		return "";
	}
	
	

}

enum AlarmSeverityEnum {
    CRITICAL("1"),
    MAJOR("2"), 
    MINOR("3"), 
    INFORMATIONAL("4"), 
    NONE("0");

    private final String severity;

    AlarmSeverityEnum(String severity) {
        this.severity = severity;
    }

    public String severity() {
        return severity;
    }
}

class LoggerProperties {

    
public static final String MDC_APPNAME = "AppName";
public static final String MDC_REST_PATH = "RestPath";
public static final String MDC_REST_METHOD = "RestMethod";
public static final String INSTANCE_UUID = "instance_uuid";
public static final String MDC_CLASS_NAME = "class";
public static final String MDC_LOGIN_ID = "LoginId";
public static final String MDC_TIMER = "Timer";
public static final String PARTNER_NAME = "PartnerName";
public static final String FULL_URL = "Full-URL";
public static final String AUDITLOG_BEGIN_TIMESTAMP = "AuditLogBeginTimestamp";
public static final String AUDITLOG_END_TIMESTAMP = "AuditLogEndTimestamp";
public static final String METRICSLOG_BEGIN_TIMESTAMP = "MetricsLogBeginTimestamp";
public static final String METRICSLOG_END_TIMESTAMP = "MetricsLogEndTimestamp";
public static final String CLIENT_IP_ADDRESS = "ClientIPAddress";
public static final String STATUS_CODE = "StatusCode";
public static final String RESPONSE_CODE = "ResponseCode";

public static final String HTTP = "HTTP";
public static final String HTTPS = "HTTPS";
public static final String UNKNOWN = "Unknown";
public static final String PROTOCOL = "PROTOCOL";
public static final String USERAGENT_NAME = "user-agent";
public static final String USER_ATTRIBUTE_NAME = "user_attribute_name";


private LoggerProperties(){}

private static Properties properties;

private static String propertyFileName = "logger.properties";

private static final Object lockObject = new Object();

//private static final EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(LoggerProperties.class);

/**
 * Gets the property value for the specified key. If a value is found, leading
 * and trailing space is trimmed.
 *
 * @param property
 *            Property key
 * @return Value for the named property; null if the property file was not
 *         loaded or the key was not found.
 */
public static String getProperty(String property) {
    if (properties == null) {
        synchronized (lockObject) {
            try {
                if (!initialize()) {
//                  logger.error(EELFLoggerDelegate.errorLogger, "Failed to read property file " + propertyFileName);
                    return null;
                }
            } catch (IOException e) {
//              logger.error(EELFLoggerDelegate.errorLogger, "Failed to read property file " + propertyFileName ,e);
                return null;
            }
        }
    }
    String value = properties.getProperty(property);
    if (value != null)
        value = value.trim();
    return value;
}

/**
 * Reads properties from a portal.properties file on the classpath.
 * 
 * Clients do NOT need to call this method. Clients MAY call this method to test
 * whether the properties file can be loaded successfully.
 * 
 * @return True if properties were successfully loaded, else false.
 * @throws IOException
 *             On failure
 */
private static boolean initialize() throws IOException {
    if (properties != null)
        return true;
    InputStream in = LoggerProperties.class.getClassLoader().getResourceAsStream(propertyFileName);
    if (in == null)
        return false;
    properties = new Properties();
    try {
        properties.load(in);
    } finally {
        in.close();
    }
    return true;
}
}