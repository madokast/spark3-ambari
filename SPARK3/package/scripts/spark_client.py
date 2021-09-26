#!/usr/bin/python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os
import sys
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.constants import StackFeature
from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.core.logger import Logger
from resource_management.core import shell
from setup_spark import setup_spark


class SparkClient(Script):
  def install(self, env):
    import params
    env.set_params(params)
    Logger.info("spark_home = {0}".format(params.spark_home))

    if os.path.exists(params.spark_home):
      return

    Execute("rm -rf /tmp/seiois")
    Execute("mkdir /tmp/seiois")
    Execute("mkdir /tmp/seiois/spark3")
    Execute("mkdir /tmp/seiois/spark3/unzip")

    # download
    Execute("wget --no-check-certificate {0} -O /tmp/seiois/spark3/spark3.tgz".format(params.download_path))

    # unzip
    Execute("tar -zxf /tmp/seiois/spark3/spark3.tgz -C /tmp/seiois/spark3/unzip")

    # find path
    unzip_path = os.listdir("/tmp/seiois/spark3/unzip")[0]
    unzip_path = os.path.join("/tmp/seiois/spark3/unzip",unzip_path)

    # rename
    Execute("mv {0} {1}".format(unzip_path, params.spark_home))

    self.configure(env)

  def configure(self, env, upgrade_type=None, config_dir=None):
    import params
    env.set_params(params)
    
    setup_spark(env, 'client', upgrade_type=upgrade_type, action = 'config')

  def status(self, env):
    raise ClientComponentHasNoStatus()

if __name__ == "__main__":
  SparkClient().execute()

