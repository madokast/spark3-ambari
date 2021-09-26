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

import sys
import fileinput
import shutil
import os
import socket

from urlparse import urlparse
from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.logger import Logger
from resource_management.core import shell
from resource_management.core.source import Template, InlineTemplate
from resource_management.core.resources.system import Directory, File
from resource_management.libraries.functions.generate_logfeeder_input_config import generate_logfeeder_input_config
from resource_management.libraries.resources.properties_file import PropertiesFile
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.constants import StackFeature
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions import lzo_utils
from resource_management.libraries.resources.xml_config import XmlConfig

def setup_spark(env, type, upgrade_type = None, action = None):
  import params

  # ensure that matching LZO libraries are installed for Spark
  lzo_utils.install_lzo_if_needed()

  Directory([params.spark_pid_dir, params.spark_log_dir],
            owner=params.spark_user,
            group=params.user_group,
            mode=0775,
            create_parents = True
  )
  if type == 'server' and action == 'config':
    Directory(params.spark3_lib_dir,
              owner=params.spark_user,
              group=params.user_group,
              create_parents = True,
              mode=0775
    )

    Directory(params.spark_history_store_path,
              owner=params.spark_user,
              group=params.user_group,
              create_parents = True,
              mode=0775
    )

    params.HdfsResource(params.spark_hdfs_user_dir,
                       type="directory",
                       action="create_on_execute",
                       owner=params.spark_user,
                       mode=0775
    )

    if not params.whs_dir_protocol or params.whs_dir_protocol == urlparse(params.default_fs).scheme:
    # Create Spark Warehouse Dir
      params.HdfsResource(params.spark_warehouse_dir,
                          type="directory",
                          action="create_on_execute",
                          owner=params.spark_user,
                          mode=0777
      )

    params.HdfsResource(None, action="execute")



  spark3_defaults = dict(params.config['configurations']['spark3-defaults'])

  if params.security_enabled:
    spark3_defaults.pop("history.server.spnego.kerberos.principal")
    spark3_defaults.pop("history.server.spnego.keytab.file")
    spark3_defaults['spark.history.kerberos.principal'] = spark3_defaults['spark.history.kerberos.principal'].replace('_HOST', socket.getfqdn().lower())

  PropertiesFile(format("{spark_conf}/spark-defaults.conf"),
    properties = spark3_defaults,
    key_value_delimiter = " ",
    owner=params.spark_user,
    group=params.spark_group,
    mode=0644
  )

  # create spark-env.sh in etc/conf dir
  File(os.path.join(params.spark_conf, 'spark-env.sh'),
       owner=params.spark_user,
       group=params.spark_group,
       content=InlineTemplate(params.spark_env_sh),
       mode=0644,
  )

  #create log4j.properties in etc/conf dir
  File(os.path.join(params.spark_conf, 'log4j.properties'),
       owner=params.spark_user,
       group=params.spark_group,
       content=params.spark_log4j_properties,
       mode=0644,
  )

  #create metrics.properties in etc/conf dir
  File(os.path.join(params.spark_conf, 'metrics.properties'),
       owner=params.spark_user,
       group=params.spark_group,
       content=InlineTemplate(params.spark_metrics_properties),
       mode=0644
  )

  if params.is_hive_installed:
    XmlConfig("hive-site.xml",
          conf_dir=params.spark_conf,
          configurations=params.spark_hive_properties,
          owner=params.spark_user,
          group=params.spark_group,
          mode=0644)

  if params.has_spark_thriftserver:
    spark3_thrift_sparkconf = dict(params.config['configurations']['spark3-thrift-sparkconf'])

    if params.security_enabled and 'spark.yarn.principal' in spark3_thrift_sparkconf:
      spark3_thrift_sparkconf['spark.yarn.principal'] = spark3_thrift_sparkconf['spark.yarn.principal'].replace('_HOST', socket.getfqdn().lower())

    PropertiesFile(params.spark_thrift_server_conf_file,
      properties = spark3_thrift_sparkconf,
      owner = params.hive_user,
      group = params.user_group,
      key_value_delimiter = " ",
      mode=0644
    )

  effective_version = params.version if upgrade_type is not None else params.stack_version_formatted
  if effective_version:
    effective_version = format_stack_version(effective_version)

  if params.spark_thrift_fairscheduler_content and effective_version and check_stack_feature(StackFeature.SPARK_16PLUS, effective_version):
    # create spark-thrift-fairscheduler.xml
    File(os.path.join(params.spark_conf,"spark-thrift-fairscheduler.xml"),
      owner=params.spark_user,
      group=params.spark_group,
      mode=0755,
      content=InlineTemplate(params.spark_thrift_fairscheduler_content)
    )
