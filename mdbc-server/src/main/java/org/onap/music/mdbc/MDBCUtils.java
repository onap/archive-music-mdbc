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

import java.io.*;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.logging.format.AppMessages;
import org.onap.music.logging.format.ErrorSeverity;
import org.onap.music.logging.format.ErrorTypes;
import org.onap.music.mdbc.tables.Operation;
import org.onap.music.mdbc.tables.StagingTable;

import javassist.bytecode.Descriptor.Iterator;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;

public class MDBCUtils {
        /** Write the object to a Base64 string. */
    public static String toString( Serializable o ) throws IOException {
    	//TODO We may want to also compress beside serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(o);
            oos.close();
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        }
        finally{
            baos.close();
        }
    }
    
    public static String toString( JSONObject o) throws IOException {
    	//TODO We may want to also compress beside serialize
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( o );
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
    
    /** Read the object from Base64 string. */
    public static Object fromString( String s ) throws IOException ,
                                                        ClassNotFoundException {
         byte [] data = Base64.getDecoder().decode( s );
         ObjectInputStream ois = new ObjectInputStream( 
                                         new ByteArrayInputStream(  data ) );
         Object o  = ois.readObject();
         ois.close();
         return o;
    }

    public static void saveToFile(String serializedContent, String filename, EELFLoggerDelegate logger) throws IOException {
        try (PrintWriter fout = new PrintWriter(filename)) {
            fout.println(serializedContent);
        } catch (FileNotFoundException e) {
            if(logger!=null){
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.IOERROR, ErrorTypes.UNKNOWN, ErrorSeverity.CRITICAL);
            }
            else {
                e.printStackTrace();
            }
            throw e;
        }
    }

}
