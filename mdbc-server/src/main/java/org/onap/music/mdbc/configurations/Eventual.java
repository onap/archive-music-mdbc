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
package org.onap.music.mdbc.configurations;

import java.util.List;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.Range;

/**
 * This class represents meta information of tables categorized as eventually consistent
 */
public class Eventual {
    
    private transient static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Eventual.class);

    protected List<Range> ranges;

    public Eventual(List<Range> ranges) {
        super();
        this.ranges = ranges;
    }

    public List<Range> getRanges() {
        return ranges;
    }

    public void setRanges(List<Range> ranges) {
        this.ranges = ranges;
    }



}
