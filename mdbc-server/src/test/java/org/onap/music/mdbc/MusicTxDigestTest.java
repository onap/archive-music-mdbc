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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;
import org.onap.music.mdbc.tables.MusicTxDigest;
import org.onap.music.mdbc.tables.StagingTable;

public class MusicTxDigestTest {

	@Test
	public void test() throws Exception {
		MusicTxDigest txDigest = new MusicTxDigest(null);
		String t1 = "rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABc3IAGW9yZy5vbmFwLm11c2ljLm1kYmMuUmFuZ2UWWoOV+3nB2AIAAUwABXRhYmxldAASTGphdmEvbGFuZy9TdHJpbmc7eHB0AAdwZXJzb25zc3IAJ29yZy5vbmFwLm11c2ljLm1kYmMudGFibGVzLlN0YWdpbmdUYWJsZWk84G3L4tunAgABTAAKb3BlcmF0aW9uc3QAFUxqYXZhL3V0aWwvQXJyYXlMaXN0O3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAV3BAAAAAVzcgAkb3JnLm9uYXAubXVzaWMubWRiYy50YWJsZXMuT3BlcmF0aW9u7yJhSJSWe0ACAANMAANLRVlxAH4AA0wAB05FV19WQUxxAH4AA0wABFRZUEV0ACpMb3JnL29uYXAvbXVzaWMvbWRiYy90YWJsZXMvT3BlcmF0aW9uVHlwZTt4cHQAJHsiUGVyc29uSUQiOjEsIkxhc3ROYW1lIjoiTWFydGluZXoifXQAWXsiQWRkcmVzcyI6IktBQ0IiLCJQZXJzb25JRCI6MSwiRmlyc3ROYW1lIjoiSnVhbiIsIkNpdHkiOiJBVExBTlRBIiwiTGFzdE5hbWUiOiJNYXJ0aW5leiJ9fnIAKG9yZy5vbmFwLm11c2ljLm1kYmMudGFibGVzLk9wZXJhdGlvblR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAZJTlNFUlRzcQB+AAt0ACR7IlBlcnNvbklEIjoxLCJMYXN0TmFtZSI6Ik1hcnRpbmV6In10AFl7IkFkZHJlc3MiOiJLQUNCIiwiUGVyc29uSUQiOjEsIkZpcnN0TmFtZSI6Ikp1YW4iLCJDaXR5IjoiQVRMQU5UQSIsIkxhc3ROYW1lIjoiTWFydGluZXoifX5xAH4AEHQABkRFTEVURXNxAH4AC3QAIXsiUGVyc29uSUQiOjIsIkxhc3ROYW1lIjoiU21pdGgifXQAWXsiQWRkcmVzcyI6IkdOT0MiLCJQZXJzb25JRCI6MiwiRmlyc3ROYW1lIjoiSk9ITiIsIkNpdHkiOiJCRURNSU5TVEVSIiwiTGFzdE5hbWUiOiJTbWl0aCJ9cQB+ABJzcQB+AAt0ACF7IlBlcnNvbklEIjoyLCJMYXN0TmFtZSI6IlNtaXRoIn10AFl7IkFkZHJlc3MiOiJHTk9DIiwiUGVyc29uSUQiOjIsIkZpcnN0TmFtZSI6IkpPU0giLCJDaXR5IjoiQkVETUlOU1RFUiIsIkxhc3ROYW1lIjoiU21pdGgifX5xAH4AEHQABlVQREFURXNxAH4AC3QAIXsiUGVyc29uSUQiOjIsIkxhc3ROYW1lIjoiU21pdGgifXQAWXsiQWRkcmVzcyI6IkdOT0MiLCJQZXJzb25JRCI6MiwiRmlyc3ROYW1lIjoiSk9ITiIsIkNpdHkiOiJCRURNSU5TVEVSIiwiTGFzdE5hbWUiOiJTbWl0aCJ9cQB+AB94eA==";
    	HashMap<Range, StagingTable> digest = (HashMap<Range, StagingTable>) MDBCUtils.fromString(t1);
    	txDigest.replayTxDigest(digest);
	}
	
	@Test
	public void testEmptyDigest() throws Exception {
		String t1 = "rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAB3CAAAABAAAAAAeA==";
		MusicTxDigest txDigest = new MusicTxDigest(null);
		HashMap<Range, StagingTable> digest = (HashMap<Range, StagingTable>) MDBCUtils.fromString(t1);
		txDigest.replayTxDigest(digest);
	}

}
