/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package configmanager

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/log"
)

var (
	// configPath stores the config file path
	configPath string
	// configLock controls the stored config
	configLock sync.RWMutex
	// conf keeps the mosn config
	conf effectiveConfig
	// configLoadFunc can be replaced by load config extension
	configLoadFunc ConfigLoadFunc = DefaultConfigLoad
)

// protetced configPath, read only
func GetConfigPath() string {
	return configPath
}

// ConfigLoadFunc parse a input(usually file path) into a mosn config
type ConfigLoadFunc func(path string) *v2.MOSNConfig

// RegisterConfigLoadFunc can replace a new config load function instead of default
func RegisterConfigLoadFunc(f ConfigLoadFunc) {
	configLoadFunc = f
}

func DefaultConfigLoad(path string) *v2.MOSNConfig {

	log.StartLogger.Infof("load config from :  %s, actual path: %s", path, os.Getenv("CONFIG_URL"))

	content, err := ioutil.ReadFile(path)
	cfg := &v2.MOSNConfig{}
	bodyStr := ""
	if err != nil {
		//log.StartLogger.Fatalf("[config] [default load] load config failed, error: %v", err)
		log.StartLogger.Errorf("[config] default load load local config failed, error %v", err)
		log.StartLogger.Infof("[config] trying to get config from remote")
		req, _ := http.NewRequest("GET", os.Getenv("CONFIG_URL"), nil)
		res, _ := http.DefaultClient.Do(req)
		//contentFromRemote, err := http.Get(path)
		//if err != nil {
		//	log.StartLogger.Fatalf("[config] get config from remote, %v, %v", res, err)
		//}
		defer res.Body.Close()
		content, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.StartLogger.Fatalf("read fail")
		}
		bodyStr = string(content)
		log.StartLogger.Infof("[config] get config %v", string(content))

	} else {
		println(content)
	}

	//cfg := &v2.MOSNConfig{}

	//if yamlFormat(path) {
	//	bytes, err := yaml.YAMLToJSON(content)
	//	if err != nil {
	//		log.StartLogger.Fatalf("[config] [default load] translate yaml to json error: %v", err)
	//	}
	//	content = bytes
	//}
	// translate to lower case
	err = json.Unmarshal([]byte(bodyStr), cfg)
	if err != nil {
		log.StartLogger.Fatalf("[config] [default load] json unmarshal config failed, error: %v", err)
	}
	return cfg

}

// Load config file and parse
func Load(path string) *v2.MOSNConfig {
	configPath, _ = filepath.Abs(path)
	cfg := configLoadFunc(path)
	return cfg
}

func yamlFormat(path string) bool {
	ext := filepath.Ext(path)
	if ext == ".yaml" || ext == ".yml" {
		return true
	}
	return false
}
