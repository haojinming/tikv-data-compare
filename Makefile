# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

default: build

GO             := GO111MODULE=on go
BUILD_BIN_PATH := $(shell pwd)/bin

#### Build ####
build: 
	$(GO) build -tags codes -gcflags "all=-N -l" -o $(BUILD_BIN_PATH)/tikv-data-compare *.go

release: 
	$(GO) build -tags codes -o $(BUILD_BIN_PATH)/tikv-data-compare *.go
 
#### Clean up ####

clean:
	# Cleaning building files...
	rm -rf $(BUILD_BIN_PATH)

.PHONY: clean clean-build
