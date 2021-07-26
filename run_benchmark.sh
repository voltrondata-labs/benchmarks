#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Cloning the benchmarks
git clone https://github.com/rahul799/benchmarks.git 
git checkout skyhookdm-workflow

# Installing conbench Dependencies
yum install postgresql-devel -y

# Setting up benchmark
python3.9 setup.py develop
pip3.9 install -r requirements-test.txt
pip3.9 install -r requirements-build.txt

# installing conbench
pip3.9 install https://github.com/ursacomputing/conbench/archive/main.zip
pip3.9 install coveralls

# installing pyarrow
pip3.9 install --upgrade /pyarrow-*.whl

# fixing python not found issue
cp /usr/local/bin/python3.9 /usr/local/bin/python

# Integrate Ceph Cluster
sh integration_ceph.sh
