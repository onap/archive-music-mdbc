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

public enum SQLOperation {
    INSERT(SQLOperationType.WRITE), SELECT(SQLOperationType.READ), UPDATE(SQLOperationType.WRITE),
    DELETE(SQLOperationType.WRITE), TABLE(SQLOperationType.TABLE);

    SQLOperationType opType;

    public SQLOperationType getOperationType() {
        return this.opType;
    }

    SQLOperation(SQLOperationType operation) {
        this.opType = operation;
    }

}
