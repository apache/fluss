#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "jsonschema==4.26.0",
#   "PyYAML==6.0.3",
# ]
# ///

import logging
import json
import yaml
from jsonschema import Draft7Validator

from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def get_parent_dir(file, steps):
    parent_path = Path(file).resolve()
    for _ in range(steps):
        parent_path = parent_path.parent
    return parent_path


root_dir = get_parent_dir(__file__, 4)
helm_dir = root_dir / "helm"
schema_meta_file = str(helm_dir / "values.schema.schema.json")
schema_file = str(helm_dir / "values.schema.json")
values_file = str(helm_dir / "values.yaml")


def validate_values_with_schema(schema_file: str, value_file: str):
    log.info(f"Validating {value_file}")
    with open(schema_file) as f:
        schema_values = json.load(f)

    if value_file.endswith("json"):
        with open(value_file) as f:
            values = json.load(f)
    else:
        with open(value_file) as f:
            values = yaml.safe_load(f)

    validator = Draft7Validator(schema_values)
    errors = sorted(validator.iter_errors(values), key=lambda e: e.path)

    if errors:
        for e in errors:
            log.error(e.message)
    else:
        log.info(f"{value_file} is valid")


if __name__ == "__main__":
    log.info("Validating Helm chart values")
    # validate values.schema.json against values.schema.schema.json
    validate_values_with_schema(schema_file=schema_meta_file, value_file=schema_file)
    # validate values.yaml against values.schema.json
    validate_values_with_schema(schema_file=schema_file, value_file=values_file)
