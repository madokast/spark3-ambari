#!/usr/bin/env python

'''
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
'''
import socket
import tarfile
import time
import os
from contextlib import closing

from resource_management.libraries.script.script import Script
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.functions.copy_tarball import copy_to_hdfs, get_tarball_paths
from resource_management.libraries.functions import format
from resource_management.core.resources.system import File, Execute
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.constants import StackFeature
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.core.shell import as_sudo
from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.logger import Logger

CHECK_COMMAND_TIMEOUT_DEFAULT = 60.0

def make_tarfile(output_filename, source_dir):
  try:
    os.remove(output_filename)
  except OSError:
    pass
  parent_dir=os.path.dirname(output_filename)
  if not os.path.exists(parent_dir):
    os.makedirs(parent_dir)
  os.chmod(parent_dir, 0711)
  with closing(tarfile.open(output_filename, "w:gz")) as tar:
    for file in os.listdir(source_dir):
      tar.add(os.path.join(source_dir,file),arcname=file)
  os.chmod(output_filename, 0644)


def spark_service(name, upgrade_type=None, action=None):
  import params

  if action == 'start':

    effective_version = params.version if upgrade_type is not None else params.stack_version_formatted
    if effective_version:
      effective_version = format_stack_version(effective_version)

    if name == 'jobhistoryserver' and effective_version and check_stack_feature(StackFeature.SPARK_16PLUS, effective_version):
      # create spark history directory
      params.HdfsResource(params.spark_history_dir,
                          type="directory",
                          action="create_on_execute",
                          owner=params.spark_user,
                          group=params.user_group,
                          mode=0777,
                          recursive_chmod=True
                          )
      params.HdfsResource(None, action="execute")

    if params.security_enabled:
      spark_kinit_cmd = format("{kinit_path_local} -kt {spark_kerberos_keytab} {spark_principal}; ")
      Execute(spark_kinit_cmd, user=params.spark_user)

    if name == 'jobhistoryserver':

      historyserver_no_op_test = as_sudo(["test", "-f", params.spark_history_server_pid_file]) + " && " + as_sudo(["pgrep", "-F", params.spark_history_server_pid_file])
      try:
        Execute(params.spark_history_server_start,
                user=params.spark_user,
                environment={'JAVA_HOME': params.java_home},
                not_if=historyserver_no_op_test)
      except:
        show_logs(params.spark_log_dir, user=params.spark_user)
        raise

  elif action == 'stop':
    if name == 'jobhistoryserver':
      try:
        Execute(format('{spark_history_server_stop}'),
                user=params.spark_user,
                environment={'JAVA_HOME': params.java_home}
        )
      except:
        show_logs(params.spark_log_dir, user=params.spark_user)
        raise
      File(params.spark_history_server_pid_file,
        action="delete"
      )



