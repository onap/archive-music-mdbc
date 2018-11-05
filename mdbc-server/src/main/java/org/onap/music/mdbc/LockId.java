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

public class LockId {
    private String primaryKey;
    private String domain;
    private String lockReference;

    public LockId(String primaryKey, String domain, String lockReference){
        this.primaryKey = primaryKey;
        this.domain = domain;
        if(lockReference == null) {
            this.lockReference = "";
        }
        else{
            this.lockReference = lockReference;
        }
    }

    public String getFullyQualifiedLockKey(){
        return this.domain+"."+this.primaryKey;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getLockReference() {
        return lockReference;
    }

    public void setLockReference(String lockReference) {
        this.lockReference = lockReference;
    }
}
