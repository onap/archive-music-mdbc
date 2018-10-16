package com.att.research.mdbc;

import java.io.*;
import java.util.Base64;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.logging.format.AppMessages;
import com.att.research.logging.format.ErrorSeverity;
import com.att.research.logging.format.ErrorTypes;
import org.json.JSONObject;

public class MDBCUtils {
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
