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

package cmd

import (
	"os"
	"strconv"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdClusterUse   = "cluster [COMMAND]"
	cmdClusterShort = "Manage cluster components"
)

func (cmd *ChubaoFSCmd) newClusterCmd(client *master.MasterClient) *cobra.Command {
	var clusterCmd = &cobra.Command{
		Use:   cmdClusterUse,
		Short: cmdClusterShort,
	}
	clusterCmd.AddCommand(
		newClusterInfoCmd(client),
		newClusterStatCmd(client),
		newClusterFreezeCmd(client),
		newClusterSetThresholdCmd(client),
	)
	return clusterCmd
}

const (
	cmdClusterInfoShort      = "Show cluster summary information"
	cmdClusterStatShort      = "Show cluster status information"
	cmdClusterFreezeShort    = "Freeze cluster"
	cmdClusterThresholdShort = "Set memory threshold of metanodes"
)

func newClusterInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var cv *proto.ClusterView
			if cv, err = client.AdminAPI().GetCluster(); err != nil {
				errout("Get cluster info fail:\n%v\n", err)
				os.Exit(1)
			}
			stdout("[Cluster]\n")
			stdout(formatClusterView(cv))
			stdout("\n")
		},
	}
	return cmd
}

func newClusterStatCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpStatus,
		Short: cmdClusterStatShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var cs *proto.ClusterStatInfo
			if cs, err = client.AdminAPI().GetClusterStat(); err != nil {
				errout("Get cluster info fail:\n%v\n", err)
				os.Exit(1)
			}
			stdout("[Cluster Status]\n")
			stdout(formatClusterStat(cs))
			stdout("\n")
		},
	}
	return cmd
}

func newClusterFreezeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpFreeze + " [ENABLE]",
		Short: cmdClusterFreezeShort,
		Long: `Turn on or off the automatic allocation of the data partitions. 
			"If freeze == true, then we WILL automatically allocate new data partitions for the volume when:
		1. the used space is below the max capacity,
		2. and the number of r&w data partition is less than 20.
		
		If freeze == false, we WILL NOT do that`,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var enable bool
			if enable, err = strconv.ParseBool(args[0]); err != nil {
				errout("Parse bool fail: %v\n", err)
				os.Exit(1)
			}
			if err = client.AdminAPI().IsFreezeCluster(enable); err != nil {
				errout("Failed: %v\n", err)
				os.Exit(1)
			}
			if enable {
				stdout("Freeze cluster successful!\n")
			} else {
				stdout("Unfreeze cluster successful!\n")
			}
		},
	}
	return cmd
}

func newClusterSetThresholdCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpSetThreshold + " [THRESHOLD]",
		Short: cmdClusterThresholdShort,
		Long: `Set the threshold of the memory usage on each meta node.
               If the memory usage reaches this threshold, all the mata partition will be readOnly.`,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var threshold float64
			if threshold, err = strconv.ParseFloat(args[0], 64); err != nil {
				errout("Parse Float fail: %v\n", err)
				os.Exit(1)
			}
			if err = client.AdminAPI().SetMetaNodeThreshold(threshold); err != nil {
				errout("Failed: %v\n", err)
				os.Exit(1)
			}
			stdout("MetaNode threshold is set to %v!\n", threshold)
		},
	}
	return cmd
}
