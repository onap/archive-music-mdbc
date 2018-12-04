#!/bin/sh
#
# ============LICENSE_START=====================================================
# Copyright (c) 2018 AT&T Intellectual Property. All rights reserved.
# ==============================================================================
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============LICENSE_END=======================================================
#

echo "Creating test database and user . . ."
mysql -uroot -p$MYSQL_ROOT_PASSWORD << 'EOF' || exit 1
DROP DATABASE IF EXISTS `test`;
CREATE DATABASE /*!32312 IF NOT EXISTS*/ `test` /*!40100 DEFAULT CHARACTER SET latin1 */;
DELETE FROM mysql.user WHERE User='test';
CREATE USER 'test';
GRANT ALL ON test.* TO 'test' identified by 'password' with GRANT OPTION;
FLUSH PRIVILEGES;
EOF
