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
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END======================================================
 */
package org.onap.music.mdbc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.onap.music.logging.EELFLoggerDelegate;

public class Utils {


    public static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Utils.class);

    static Properties retrieveMdbcProperties() {
        Properties pr = null;
        try {
            pr = new Properties();
            pr.load(Utils.class.getResourceAsStream("/mdbc.properties"));
        } catch (IOException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Could not load property file: " + e.getMessage());
        }
        return pr;
    }

    public static String getDefaultMusicMixin() {
        Properties pr = retrieveMdbcProperties();
        if (pr == null)
            return null;
        String defaultMusicMixin = pr.getProperty("DEFAULT_MUSIC_MIXIN");
        return defaultMusicMixin;
    }

    public static String getDefaultDBMixin() {
        Properties pr = retrieveMdbcProperties();
        if (pr == null)
            return null;
        String defaultMusicMixin = pr.getProperty("DEFAULT_DB_MIXIN");
        return defaultMusicMixin;
    }

    public static void registerDefaultDrivers() {
        Properties pr = null;
        try {
            pr = new Properties();
            pr.load(Utils.class.getResourceAsStream("/mdbc.properties"));
        } catch (IOException e) {
            logger.error("Could not load property file > " + e.getMessage());
        }

        String drivers = pr.getProperty("DEFAULT_DRIVERS");
        for (String driver : drivers.split("[ ,]")) {
            logger.info(EELFLoggerDelegate.applicationLogger, "Registering jdbc driver '" + driver + "'");
            try {
                Class.forName(driver.trim());
            } catch (ClassNotFoundException e) {
                logger.error(EELFLoggerDelegate.errorLogger, "Driver class " + driver + " not found.");
            }
        }
    }
}
