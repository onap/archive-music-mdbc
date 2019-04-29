/*
 * ============LICENSE_START====================================================
 * org.onap.music.mdbc
 * =============================================================================
 * Copyright (C) 2019 AT&T Intellectual Property. All rights reserved.
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

package org.onap.music.mdbc.mixins;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.websocket.RemoteEndpoint.Async;
import org.onap.music.logging.EELFLoggerDelegate;

public class AsyncUpdateHandler {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(AsyncUpdateHandler.class);

    Object finishMonitor;
    Object taskMonitor;
    AtomicBoolean pendingUpdate;
    Runnable handler;

    AsyncUpdateHandler(Runnable handlerToRun){
        handler=handlerToRun;
        finishMonitor=new Object();
        taskMonitor = new Object();
        pendingUpdate=new AtomicBoolean(false);
        createUpdateExecutor();
    }

    void createUpdateExecutor(){
        Runnable newRunnable = ()-> UpdateExecutor();
        Thread newThread = new Thread(newRunnable);
        newThread.setDaemon(true);
        newThread.start();
    }

    public void processNewUpdate(){
        pendingUpdate.set(true);
        synchronized (taskMonitor) {
            taskMonitor.notifyAll();
        }
    }

    void UpdateExecutor(){
        while(true) {
            synchronized (taskMonitor) {
                try {
                    if(!pendingUpdate.get()){
                        taskMonitor.wait();
                    }
                } catch (InterruptedException e) {
                    logger.error("Update Executor received an interrup exception");
                }
            }
            startUpdate();
        }
    }

    void startUpdate(){
        synchronized (finishMonitor) {
            //Keep running until there are no more requests
            while (pendingUpdate.getAndSet(false)) {
                if(handler!=null){
                    handler.run();
                }
            }
            finishMonitor.notifyAll();
        }

    }

    public void waitForAllPendingUpdates(){
        //Wait until there are no more requests and the thread is not longer running
        //We could use a join, but given that the thread could be temporally null, then it is easier to
        //separate the wait from the thread that is running
        synchronized(finishMonitor){
            while(pendingUpdate.get()) {
                try {
                    finishMonitor.wait();
                } catch (InterruptedException e) {
                    logger.error("waitForAllPendingUpdate received exception");
                }
            }
        }
    }
}
