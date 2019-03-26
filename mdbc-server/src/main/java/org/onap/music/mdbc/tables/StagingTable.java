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
package org.onap.music.mdbc.tables;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import javax.validation.constraints.Null;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.proto.ProtoDigest.Digest.CompleteDigest;
import org.onap.music.mdbc.proto.ProtoDigest.Digest.CompleteDigest.Builder;
import org.onap.music.mdbc.proto.ProtoDigest.Digest.Row;
import org.onap.music.mdbc.proto.ProtoDigest.Digest.Row.OpType;

public class StagingTable {

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(StagingTable.class);
    private ArrayList<Operation> operations;
    boolean builderInitialized;
	Builder digestBuilder;
    Builder eventuallyBuilder;
	Set<Range> eventuallyConsistentRanges;

	public StagingTable(){
        this(new HashSet<>());
	    logger.debug("Creating staging table with no parameters, most likely this is wrong, unless you are testing");
    }

    public StagingTable(StagingTable other) throws CloneNotSupportedException {
	    if(other==null){
	        throw new NullPointerException("Invalid constructor parameter passed, it is null");
        }
	    //TODO this is a highly inefficient deep copy, please don't use in prod
        operations=null;
        if(other.operations!=null) {
            Iterator<Operation> iterator = other.operations.iterator();
            operations = new ArrayList<>();
            while (iterator.hasNext()) {
                operations.add((Operation) iterator.next().clone());
            }
        }
        builderInitialized=other.builderInitialized;
        digestBuilder=null;
        if(other.digestBuilder!=null) {
            CompleteDigest build = other.digestBuilder.build();
            digestBuilder = build.toBuilder();
        }
        eventuallyBuilder=null;
        if(other.eventuallyBuilder!=null) {
            CompleteDigest build2 = other.digestBuilder.build();
            eventuallyBuilder = build2.toBuilder();
        }
        eventuallyConsistentRanges=null;
        if(other.eventuallyConsistentRanges!=null) {
            eventuallyConsistentRanges = new HashSet<>();
            Iterator<Range> rangeIter = other.eventuallyConsistentRanges.iterator();
            while (rangeIter.hasNext()) {
                eventuallyConsistentRanges.add(rangeIter.next().clone());
            }
        }
    }
	
	public StagingTable(Set<Range> eventuallyConsistentRanges) {
		//operations = new ArrayList<Operation>();
        operations=null;
        builderInitialized=true;
		digestBuilder = CompleteDigest.newBuilder();
		this.eventuallyConsistentRanges=eventuallyConsistentRanges;
		eventuallyBuilder = (!this.eventuallyConsistentRanges.isEmpty())?null:CompleteDigest.newBuilder();
	}

	public StagingTable(ByteBuffer serialized) throws MDBCServiceException {
	    builderInitialized=false;
	    operations = new ArrayList<>();
	    CompleteDigest completeDigest;
        try {
            completeDigest = CompleteDigest.parseFrom(serialized);
        } catch (InvalidProtocolBufferException e) {
            throw new MDBCServiceException("Invalid serialized input to protobuf deserializer",e);
        }
        for(Row row : completeDigest.getRowsList()){
            final OpType type = row.getType();
            OperationType newType = (type==OpType.INSERT)?OperationType.INSERT:(type==OpType.DELETE)?
            OperationType.DELETE:OperationType.UPDATE;
            operations.add(new Operation(row.getTable(),newType,row.getVal(),row.getKey()));
        }
    }

    public static ByteBuffer Compress(ByteBuffer serializedStaging) throws MDBCServiceException {
	    if(serializedStaging.hasArray()) {
	        //\TODO: Use JAVA 11 to simplify this process using ByteBuffer natively
            Deflater compressor =  new Deflater();
            final byte[] inputArray = serializedStaging.array();
            compressor.setInput(inputArray);
            compressor.finish();
            ByteArrayOutputStream bos = new ByteArrayOutputStream(serializedStaging.array().length);
            byte[] buf = new byte[1024];
            try {
                while (!compressor.finished()) {
                    int i = compressor.deflate(buf);
                    bos.write(buf, 0, i);
                }
            } finally {
                compressor.end();
                try {
                    bos.close();
                } catch (IOException e) {
                    throw new MDBCServiceException("Error closing ByetArrayOutputStream:",e);
                }
            }
            byte[] output = bos.toByteArray();
            logger.debug("Staging table compressed from: "+inputArray.length+" to "+output.length);
            return ByteBuffer.wrap(output);
        }
        else{
            throw new MDBCServiceException("Byte buffer was not created correctly, it should wrap an array");
        }
    }

    public static ByteBuffer Decompress(ByteBuffer compressedStaging) throws MDBCServiceException {
	    if(compressedStaging.hasArray()) {
            //\TODO: Use JAVA 11 to simplify this process using ByteBuffer natively
            Inflater decompressor = new Inflater();
            byte[] inputArray = compressedStaging.array();
            decompressor.setInput(inputArray);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(inputArray.length);
            byte[] buffer = new byte[1024];
            while (!decompressor.finished()) {
                int decompressSize = 0;
                try {
                    decompressSize = decompressor.inflate(buffer);
                } catch (DataFormatException e) {
                    throw new MDBCServiceException("error decompressing input data",e);
                }
                outputStream.write(buffer, 0, decompressSize);
            }
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new MDBCServiceException("Error closing output byte stream",e);
            }
            byte[] output = outputStream.toByteArray();
            return ByteBuffer.wrap(output);
        }
        else{
            throw new MDBCServiceException("Byte buffer was not created correctly, it should wrap an array");
        }
    }

    synchronized  public boolean isBuilderInitialized(){
	    return isBuilderInitialized();
    }
	
	synchronized public void addOperation(Range range, OperationType type, String newVal, String keys)
        throws MDBCServiceException {
	    if(!builderInitialized){
            throw new MDBCServiceException("This type of staging table is unmutable, please use the constructor"
                + "with no parameters");
        }
		OpType newType = (type==OperationType.INSERT)?OpType.INSERT:(type==OperationType.DELETE)?
			OpType.DELETE:OpType.UPDATE;
	    Row.Builder rowBuilder = Row.newBuilder().setTable(range.getTable()).setType(newType).setVal(newVal);
	    if(keys!=null){
	        rowBuilder.setKey(keys);
        }
	    if(eventuallyConsistentRanges!=null && eventuallyConsistentRanges.contains(range)){
	        if(eventuallyBuilder==null){
               throw new MDBCServiceException("INCONSISTENCY: trying to add eventual op with no eventual ranges");
            }
            eventuallyBuilder.addRows(rowBuilder);
        }
        else {
            digestBuilder.addRows(rowBuilder);
        }
		//operations.add(new Operation(table,type,newVal,keys));
	}
	
	synchronized public ArrayList<Operation> getOperationList() {
	    if(!builderInitialized) {
            return operations;
        }
        logger.warn("Get operation list with this type of initialization is not suggested for the"
            + "staging table");
        ArrayList<Operation> newOperations = new ArrayList<>();
        for(Row row : digestBuilder.getRowsList()){
            final OpType type = row.getType();
            OperationType newType = (type==OpType.INSERT)?OperationType.INSERT:(type==OpType.DELETE)?
                OperationType.DELETE:OperationType.UPDATE;
            newOperations.add(new Operation(row.getTable(),newType,row.getVal(),row.getKey()));
        }
        return newOperations;
    }

	synchronized public ByteBuffer getSerializedStagingAndClean() throws MDBCServiceException {
        if(!builderInitialized){
            throw new MDBCServiceException("This type of staging table is unmutable, please use the constructor"
                + "with no parameters");
        }
        byte[] bytes = digestBuilder.build().toByteArray();
	    ByteBuffer serialized = ByteBuffer.wrap(bytes);
	    digestBuilder.clear();
	    return serialized;
    }

    synchronized public ByteBuffer getSerializedEventuallyStagingAndClean() throws MDBCServiceException {
        if(!builderInitialized){
            throw new MDBCServiceException("This type of staging table is unmutable, please use the constructor"
                + "with no parameters");
        }
        if(eventuallyBuilder == null || eventuallyBuilder.getRowsCount()==0){
            return null;
        }
        byte[] bytes = eventuallyBuilder.build().toByteArray();
	    ByteBuffer serialized = ByteBuffer.wrap(bytes);
	    eventuallyBuilder.clear();
	    return serialized;
    }

    synchronized public boolean isEmpty() {
	    return (digestBuilder.getRowsCount()==0);
    }
	
	synchronized public void clear() throws MDBCServiceException {
        if(!builderInitialized){
            throw new MDBCServiceException("This type of staging table is unmutable, please use the constructor"
                + "with no parameters");
        }
		digestBuilder.clear();
	}

	synchronized public boolean areEventualContained(List<Range> ranges){
	    return eventuallyConsistentRanges.containsAll(ranges);
    }
}
