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

import java.util.List;
import org.apache.commons.lang.NotImplementedException;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.MusicInterface;

public class MusicSynchronization {
    public static void SynchronizeTables(DBInterface dbi, MusicInterface mi, List<Range> ranges) throws MDBCServiceException {
        throw new NotImplementedException("Need to be implemented");
    }

}
