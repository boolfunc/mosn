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

package cluster

import (
	"golang.org/x/tools/container/intsets"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/types"
)

type SubsetBuildMode uint8

const (
	SubsetPreIndexBuildMode SubsetBuildMode = iota
	SubsetFilterBuildMode
)

var subsetBuildMode = SubsetPreIndexBuildMode

func SetSubsetBuildMode(mode SubsetBuildMode) {
	subsetBuildMode = mode
}
func getSubsetBuildMode() SubsetBuildMode {
	return subsetBuildMode
}

func NewSubsetLoadBalancerPreIndex(info types.ClusterInfo, hostSet types.HostSet) types.LoadBalancer {
	builder := newSubsetLoadBalancerBuilder(info, hostSet)
	return builder.Build()
}

type subsetLoadBalancerBuilder struct {
	info        types.ClusterInfo
	indexer     map[string]map[string]*intsets.Sparse
	hostSet     types.HostSet
	subSetCount int64
}

func newSubsetLoadBalancerBuilder(info types.ClusterInfo, hs types.HostSet) *subsetLoadBalancerBuilder {
	b := &subsetLoadBalancerBuilder{
		hostSet: hs,
		info:    info,
	}
	b.initIndex()
	return b
}

func (b *subsetLoadBalancerBuilder) Build() *subsetLoadBalancer {

	fullLb := NewLoadBalancer(b.info, &hostSet{allHosts: b.filterHosts(nil)})
	fallbackSubset := b.createFallbackSubset(fullLb)
	subsets := b.createSubsets()
	sslb := &subsetLoadBalancer{
		lbType:         b.info.LbType(),
		stats:          b.info.Stats(),
		hostSet:        b.hostSet,
		fullLb:         fullLb,
		fallbackSubset: fallbackSubset,
		subSets:        subsets,
	}
	sslb.stats.LBSubsetsCreated.Update(b.subSetCount)
	return sslb
}

func (b *subsetLoadBalancerBuilder) initIndex() {
	subsetInfo := b.info.LbSubsetInfo()
	keys := subsetMergeKeys(subsetInfo.SubsetKeys(), subsetInfo.DefaultSubset())
	hosts := b.hostSet.Hosts()
	indexer := make(map[string]map[string]*intsets.Sparse)
	for _, key := range keys {
		valueMap := make(map[string]*intsets.Sparse)
		indexer[key] = valueMap
		for i, host := range hosts {
			value, ok := host.Metadata()[key]
			if !ok {
				continue
			}
			s, ok := valueMap[value]
			if !ok {
				s = &intsets.Sparse{}
				valueMap[value] = s
			}
			s.Insert(i)
		}
	}
	b.indexer = indexer
}

func (b *subsetLoadBalancerBuilder) createSubsets() types.LbSubsetMap {
	subSetKeys := b.info.LbSubsetInfo().SubsetKeys()
	subSets := make(types.LbSubsetMap)
	for _, subSetKey := range subSetKeys {
		for _, kvs := range b.metadataCombinations(subSetKey.Keys()) {
			entry := b.findOrCreateSubset(subSets, kvs, 0)
			hosts := b.filterHosts(kvs)
			if len(hosts) > 0 {
				entry.CreateLoadBalancer(b.info, &hostSet{allHosts: hosts})
				b.subSetCount++
			}
		}
	}
	return subSets
}

func (b *subsetLoadBalancerBuilder) selectHosts(s *intsets.Sparse) []types.Host {
	if s == nil {
		return nil
	}
	hosts := b.hostSet.Hosts()
	offsets := make([]int, 0, s.Len())
	offsets = s.AppendTo(offsets)
	ret := make([]types.Host, len(offsets))
	for i, n := range offsets {
		ret[i] = hosts[n]
	}
	return ret
}

func (b *subsetLoadBalancerBuilder) filterHosts(kvs types.SubsetMetadata) []types.Host {
	hosts := b.hostSet.Hosts()
	if len(kvs) == 0 {
		ret := make([]types.Host, len(hosts))
		copy(ret, hosts)
		return ret
	}
	var curSet *intsets.Sparse
	for _, kv := range kvs {
		key := kv.T1
		val := kv.T2
		valueMap, ok := b.indexer[key]
		if !ok {
			return make([]types.Host, 0)
		}
		set, ok := valueMap[val]
		if !ok {
			return make([]types.Host, 0)
		}
		if curSet == nil {
			curSet = &intsets.Sparse{}
			curSet.Copy(set)
		} else {
			curSet.IntersectionWith(set)
		}
	}
	return b.selectHosts(curSet)
}

func (b *subsetLoadBalancerBuilder) createFallbackSubset(fullLb types.LoadBalancer) *LBSubsetEntryImpl {
	policy := b.info.LbSubsetInfo().FallbackPolicy()
	switch policy {
	case types.NoFallBack:
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[upstream] [subset lb] subset load balancer: fallback is disabled")
		}
		return nil
	case types.AnyEndPoint:
		return &LBSubsetEntryImpl{
			children: nil,
			lb:       fullLb,
			hostSet:  b.hostSet,
		}
	case types.DefaultSubset:
		subset := &LBSubsetEntryImpl{
			children: nil,
		}
		subset.CreateLoadBalancer(b.info, &hostSet{allHosts: b.filterHosts(b.info.LbSubsetInfo().DefaultSubset())})
		return subset
	}
	return nil
}

func (b *subsetLoadBalancerBuilder) findOrCreateSubset(subsets types.LbSubsetMap, kvs types.SubsetMetadata, idx uint32) types.LBSubsetEntry {
	name := kvs[idx].T1
	value := kvs[idx].T2
	var entry types.LBSubsetEntry

	if vsMap, ok := subsets[name]; ok {
		lbEntry, ok := vsMap[value]
		if !ok {
			lbEntry = &LBSubsetEntryImpl{
				children: make(map[string]types.ValueSubsetMap),
			}
			vsMap[value] = lbEntry
			subsets[name] = vsMap
		}
		entry = lbEntry
	} else {
		entry = &LBSubsetEntryImpl{
			children: make(map[string]types.ValueSubsetMap),
		}
		subsets[name] = types.ValueSubsetMap{
			value: entry,
		}
	}
	idx++
	if idx == uint32(len(kvs)) {
		return entry
	}
	return b.findOrCreateSubset(entry.Children(), kvs, idx)
}

func (b *subsetLoadBalancerBuilder) metadataCombinations(keys []string) []types.SubsetMetadata {
	/**
	recursion iter every values to extract kv pairs (full combination)
	indexer:
	{
	    k1: {v1, v2},
	    k2: {v3},
	    k3: {v4, v5}
	    k4: {v6, v7, v8}
	}
	keys:
	[k1, k2, k3]
	return:
	[{k1, v1}, {k2, v3}, {k3, v4}]
	[{k1, v1}, {k2, v3}, {k3, v5}]
	[{k1, v2}, {k2, v3}, {k3, v4}]
	[{k1, v2}, {k2, v3}, {k3, v5}]
	*/

	return b.doMetadataCombination(keys, 0, nil)
}

func (b *subsetLoadBalancerBuilder) doMetadataCombination(keys []string, idx int, kvs types.SubsetMetadata) []types.SubsetMetadata {
	key := keys[idx]
	var ret []types.SubsetMetadata
	for value := range b.indexer[key] {
		newkvs := make(types.SubsetMetadata, len(kvs), len(kvs)+1)
		copy(newkvs, kvs)
		newkvs = append(newkvs, types.Pair{T1: key, T2: value})
		if idx+1 < len(keys) {
			ret = append(ret, b.doMetadataCombination(keys, idx+1, newkvs)...)
		} else {
			ret = append(ret, newkvs)
		}
	}
	return ret
}

func subsetMergeKeys(subSetKeys []types.SortedStringSetType, defaultSubset types.SubsetMetadata) []string {
	m := make(map[string]struct{})
	for _, keys := range subSetKeys {
		for _, key := range keys.Keys() {
			m[key] = struct{}{}
		}
	}
	for _, pair := range defaultSubset {
		m[pair.T1] = struct{}{}
	}
	ret := make([]string, 0, len(m))
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}
