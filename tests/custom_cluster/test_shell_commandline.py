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

from __future__ import absolute_import, division, print_function
import os
import pytest
import re
import tempfile

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_client_protocol_http_transport
from time import sleep
from tests.shell.util import run_impala_shell_cmd


class TestImpalaShellCommandLine(CustomClusterTestSuite):
  """Runs tests of the Impala shell by first standing up an Impala cluster with
  specific startup flags.  Then, the Impala shell is launched with specific arguments
  in a separate process.  Assertions are done by scanning the shell output and Impala
  server logs for expected strings."""

  LOG_DIR_HTTP_TRACING = tempfile.mkdtemp(prefix="http_tracing")
  LOG_DIR_HTTP_TRACING_OFF = tempfile.mkdtemp(prefix="http_tracing_off")
  IMPALA_ID_RE = "([0-9a-f]{16}:[0-9a-f]{16})"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    """Overrides all other add_dimension methods in super classes up the entire class
    hierarchy ensuring that each test in this class only get run once using the
    hs2-http protocol."""
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_http_transport())

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-log_dir={0} -v 2".format(LOG_DIR_HTTP_TRACING))
  def test_http_tracing_headers(self, vector):
    """Asserts that tracing headers are automatically added by the impala shell to
    all calls to the backend impala engine made using the hs2 over http protocol.
    The impala coordinator logs are searched to ensure these tracing headers were added
    and also were passed through to the coordinator."""
    args = ['--protocol', vector.get_value('protocol'), '-q', 'select version();profile']
    result = run_impala_shell_cmd(vector, args)

    # Shut down cluster to ensure logs flush to disk.
    sleep(5)
    self._stop_impala_cluster()

    # Ensure the query ran successfully.
    assert result.stdout.find("version()") > -1
    assert result.stdout.find("impalad version") > -1
    assert result.stdout.find("Query Runtime Profile") > -1

    request_id_base = ""
    request_id_serialnum = 0
    session_id = ""
    query_id = ""
    last_known_query_id = ""
    tracing_lines_count = 0

    request_id_re = re.compile("x-request-id=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                               "[0-9a-f]{4}-[0-9a-f]{12})-(\\d+)")
    session_id_re = re.compile("x-session-id={0}"
                               .format(TestImpalaShellCommandLine.IMPALA_ID_RE))
    query_id_re = re.compile("x-query-id={0}"
                               .format(TestImpalaShellCommandLine.IMPALA_ID_RE))
    profile_query_id_re = re.compile("Query \\(id={0}\\)"
                               .format(TestImpalaShellCommandLine.IMPALA_ID_RE))

    # Find all HTTP Connection Tracing log lines.
    with open(os.path.join(self.LOG_DIR_HTTP_TRACING, "impalad.INFO")) as log_file:
      for line in log_file:
        if line.find("HTTP Connection Tracing Headers") > -1:
          tracing_lines_count += 1

          # The impala shell builds a request_id that consists of the same randomly
          # generated uuid and a serially increasing integer appended on the end.
          # Ensure both these conditions are met.
          m = request_id_re.search(line)
          assert m is not None, \
            "did not find request id in HTTP connection tracing log line '{0}'" \
            .format(line)

          if request_id_base == "":
            # The current line is the very first HTTP connection tracing line in the logs.
            request_id_base = m.group(1)
          else:
            assert request_id_base == m.group(1), \
              "base request id expected '{0}', actual '{1}'" \
              .format(request_id_base, m.group(1))

          request_id_serialnum += 1
          assert request_id_serialnum == int(m.group(2)), \
            "request id serial number expected '{0}', actual '{1}'" \
            .format(request_id_serialnum, m.group(2))

          # The session_id is generated by impala and must be the same once it
          # appears in a tracing log line.
          m = session_id_re.search(line)
          if m is not None:
            if session_id == "":
              session_id = m.group(1)
            else:
              assert session_id == m.group(1), \
                "session id expected '{0}', actual '{1}'".format(session_id, m.group(1))

          # The query_id is generated by impala and must be the same for the
          # duration of the query.
          m = query_id_re.search(line)
          if m is None:
            query_id = ""
          else:
            if query_id == "":
              query_id = m.group(1)
              last_known_query_id = query_id
            else:
              assert query_id == m.group(1), \
                "query id expected '{0}', actual '{1}'".format(query_id, m.group(1))

    # Assert that multiple HTTP connection tracing log lines were found.
    assert tracing_lines_count > 10, \
      "did not find enough HTTP connection tracing log lines, found {0} lines" \
      .format(tracing_lines_count)

    # Ensure the last found query id matches the actual query id
    # from the impala query profile.
    m = profile_query_id_re.search(result.stdout)
    if m is not None:
      assert last_known_query_id == m.group(1), \
        "impala query profile id, expected '{0}', actual '{1}'" \
        .format(last_known_query_id, m.group(1))
    else:
      pytest.fail("did not find Impala query id in shell stdout")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-log_dir={0} -v 2".format(LOG_DIR_HTTP_TRACING_OFF))
  def test_http_tracing_headers_off(self, vector):
    """Asserts the impala shell command line parameter to prevent the addition of http
    tracing headers actually leaves out those tracing headers."""
    args = ['--protocol', vector.get_value('protocol'), '--no_http_tracing',
            '-q', 'select version();profile']
    result = run_impala_shell_cmd(vector, args)

    # Shut down cluster to ensure logs flush to disk.
    sleep(5)
    self._stop_impala_cluster()

    # Ensure the query ran successfully.
    assert result.stdout.find("version()") > -1
    assert result.stdout.find("impalad version") > -1
    assert result.stdout.find("Query Runtime Profile") > -1

    # Find all HTTP Connection Tracing log lines (there should not be any).
    with open(os.path.join(self.LOG_DIR_HTTP_TRACING_OFF, "impalad.INFO")) as log_file:
      for line in log_file:
        if line.find("HTTP Connection Tracing Headers") != -1:
          pytest.fail("found HTTP connection tracing line line: {0}".format(line))
