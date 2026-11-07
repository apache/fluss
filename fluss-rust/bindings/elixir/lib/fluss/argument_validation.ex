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

defmodule Fluss.ArgumentValidation do
  @moduledoc false
  # Generic argument checks shared by the public modules; domain checks (data types, partition
  # specs, changes) live in their own module. Each raises `ArgumentError` or returns the
  # (normalized) value.

  def validate_string!(value, _what) when is_binary(value), do: value

  def validate_string!(value, what) do
    raise ArgumentError, "expected the #{what} to be a string, got: #{inspect(value)}"
  end

  def validate_name!(name, what), do: validate_string!(name, "#{what} name")

  def validate_items!(list, field, validate_item) when is_list(list) do
    if List.improper?(list) do
      raise ArgumentError, "expected #{field} to be a proper list, got: #{inspect(list)}"
    end

    Enum.map(list, validate_item)
  end

  def validate_items!(other, field, _validate_item) do
    raise ArgumentError, "expected #{field} to be a list, got: #{inspect(other)}"
  end

  def validate_keyword!(opts, defaults) when is_list(opts), do: Keyword.validate!(opts, defaults)

  def validate_keyword!(opts, _defaults) do
    raise ArgumentError, "expected options to be a keyword list, got: #{inspect(opts)}"
  end
end
