// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"time"
)

func (c *Cluster) scheduleToCheckDiskRecoveryProgress() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkDiskRecoveryProgress()
				}
			}
			time.Sleep(time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkDiskRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDiskRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDiskRecoveryProgress occurred panic")
		}
	}()
	var diff float64
	c.fullFillReplica()
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		newBadDpIds := make([]uint64, 0)
		for _, partitionID := range badDataPartitionIds {
			partition, err := c.getDataPartitionByID(partitionID)
			if err != nil {
				continue
			}
			vol, err := c.getVol(partition.VolName)
			if err != nil {
				continue
			}
			if len(partition.Replicas) == 0 {
				continue
			}
			diff = partition.getMinus()
			if diff < util.GB && len(partition.Replicas) >= int(vol.dpReplicaNum){
				partition.isRecover = false
				partition.RLock()
				c.syncUpdateDataPartition(partition)
				partition.RUnlock()
				Warn(c.Name, fmt.Sprintf("clusterID[%v],partitionID[%v] has recovered success", c.Name, partitionID))
			} else {
				newBadDpIds = append(newBadDpIds, partitionID)
			}
		}

		if len(newBadDpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("clusterID[%v],node:disk[%v] has recovered success", c.Name, key))
			c.BadDataPartitionIds.Delete(key)
		} else {
			c.BadDataPartitionIds.Store(key, newBadDpIds)
		}

		return true
	})
}
// Add replica for the partition whose replica number is less than replicaNum
func (c *Cluster) fullFillReplica() {
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		badDiskAddr := key.(string)
		newBadParitionIds := make([]uint64, 0)
		for _, partitionID := range badDataPartitionIds {
			var isSkip bool
			var err    error
			if isSkip, err = c.checkAddDataReplica(badDiskAddr, partitionID); err != nil {
				log.LogWarnf(fmt.Sprintf("action[fullFillReplica], clusterID[%v], partitionID[%v], err[%v] ", c.Name, partitionID, err))
			}
			if !isSkip {
				newBadParitionIds = append(newBadParitionIds, partitionID)
			}
		}
		//Todo: write BadDataPartitionIds to raft log
		c.BadDataPartitionIds.Store(key, newBadParitionIds)
		return true
	})

}

func (c *Cluster) checkAddDataReplica(badDiskAddr string, partitionID uint64) (isSkip bool, err error){
	var(
		newAddr    string
		partition  *DataPartition
	)
	if partition, err = c.getDataPartitionByID(partitionID); err != nil {
		return
	}
	if int(partition.ReplicaNum) == len(partition.Replicas) {
		return
	}
	if leaderAddr := partition.getLeaderAddr(); leaderAddr == "" {
		log.LogWarnf(fmt.Sprintf("Action[checkAddReplica], partitionID[%v], no leader", partitionID))
		return
	}
	if newAddr, err = c.chooseTargetDataPartitionHost(badDiskAddr, partition); err != nil {
		return
	}
	if err = c.addDataReplica(partition, newAddr); err != nil {
		return
	}
	// Todo: What if Master changed leader before this step?
	if int(partition.ReplicaNum) > len(partition.Replicas) {
		isSkip = true
	}
	return
}

func (c *Cluster) decommissionDisk(dataNode *DataNode, badDiskPath string, badPartitions []*DataPartition) (err error) {
	msg := fmt.Sprintf("action[decommissionDisk], Node[%v] OffLine,disk[%v]", dataNode.Addr, badDiskPath)
	log.LogWarn(msg)

	for _, dp := range badPartitions {
		if err = c.decommissionDataPartition(dataNode.Addr, dp, diskOfflineErr); err != nil {
			return
		}
	}
	msg = fmt.Sprintf("action[decommissionDisk],clusterID[%v] Node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(c.Name, msg)
	return
}
