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

package org.onap.music.mdbc.tables;

import org.onap.music.mdbc.Range;

import java.util.List;

public class RangeDependency {
    final Range baseRange;
    final List<Range> dependentRanges;

    public RangeDependency(Range baseRange, List<Range> dependentRanges){
       this.baseRange=baseRange;
       this.dependentRanges=dependentRanges;
    }
    public Range getRange(){
        return baseRange;
    }
    public List<Range> dependentRanges(){
        return dependentRanges;
    }
}
