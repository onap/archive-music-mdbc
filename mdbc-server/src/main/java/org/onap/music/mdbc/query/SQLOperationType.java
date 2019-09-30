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
package org.onap.music.mdbc.query;

import org.onap.music.lockingservice.cassandra.LockType;

public enum SQLOperationType {
    //READ < WRITE < TABLE.... order important here
    READ, WRITE, TABLE; 
    
    public static SQLOperationType FromMusicLockType(LockType lockType) {
        switch (lockType) {
            case READ:
                return SQLOperationType.READ;
            case WRITE:
                return SQLOperationType.WRITE;
            default:
                return null;
        }

    }
    
    public static LockType ToMusicLockType(SQLOperationType opType) {
        switch (opType) {
            case READ:
                return LockType.READ;
            case WRITE:
                return LockType.WRITE;
            default:
                return null;
        }

    }
}
