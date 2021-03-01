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

package codecnode

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"

	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"

	echandler "github.com/chubaofs/chubaofs/util/ec"
)

func (s *CodecServer) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size
	tpObject := exporter.NewTPCnt(p.GetOpMsg())
	start := time.Now().UnixNano()
	defer func() {
		resultSize := p.Size
		p.Size = sz
		if p.IsErrPacket() {
			err = fmt.Errorf("op(%v) error(%v)", p.GetOpMsg(), string(p.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
			switch p.Opcode {
			default:
				log.LogInfo(logContent)
			}
		}
		p.Size = resultSize
		tpObject.Set(err)
	}()

	switch p.Opcode {
	case proto.OpCodecNodeHeartbeat:
		s.handleHeartbeatPacket(p)
	case proto.OpIssueMigrationTask:
		s.handleEcMigrationTask(p, c)
	default:
		p.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode)))
	}

	return
}

func (s *CodecServer) handleHeartbeatPacket(p *repl.Packet) {
	var err error
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody("ActionHeartbeat", err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}

	go func() {
		request := &proto.HeartBeatRequest{}
		response := &proto.CodecNodeHeartbeatResponse{}
		s.buildHeartBeatResponse(response)

		if task.OpCode == proto.OpCodecNodeHeartbeat {
			marshaled, _ := json.Marshal(task.Request)
			_ = json.Unmarshal(marshaled, request)
			response.Status = proto.TaskSucceeds
		} else {
			response.Status = proto.TaskFailed
			err = fmt.Errorf("illegal opcode")
			response.Result = err.Error()
		}
		task.Response = response
		if err = MasterClient.NodeAPI().ResponseCodecNodeTask(task); err != nil {
			err = errors.Trace(err, "heartbeat to master(%v) failed.", request.MasterAddr)
			log.LogErrorf(err.Error())
			return
		}
	}()

}

func (s *CodecServer) buildHeartBeatResponse(response *proto.CodecNodeHeartbeatResponse) {
	response.Status = proto.TaskSucceeds
}

func (s *CodecServer) handleEcMigrationTask(p *repl.Packet, c *net.TCPConn) {
	var err error
	req := &proto.IssueMigrationTaskRequest{}
	err = json.Unmarshal(p.Data, req)
	defer func() {
		if err != nil {
			p.PackErrorBody("ActionEcMigrationTask", err.Error())
			log.LogErrorf(err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}

	var metaConfig = &meta.MetaConfig{
		Volume:        req.VolName,
		Masters:       s.masters,
		Authenticate:  false,
		ValidateOwner: false,
		OnAsyncTaskError: func(err error) {
			return
		},
	}
	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		return
	}
	defer func() {
		metaWrapper.Close()
	}()

	var extentConfig = &stream.ExtentConfig{
		Volume:            req.VolName,
		Masters:           s.masters,
		FollowerRead:      true,
		OnAppendExtentKey: metaWrapper.AppendExtentKey,
		OnGetExtents:      metaWrapper.GetExtents,
		OnTruncate:        metaWrapper.Truncate,
	}
	var ec *stream.ExtentClient
	if ec, err = stream.NewExtentClient(extentConfig); err != nil {
		return
	}

	ecl, err := NewEcClient(req.VolName, s.masters)
	if err != nil {
		return
	}

	for _, i := range req.Inodes {
		go func(inode uint64) {
			var err error

			var extentKeys []proto.ExtentKey

			defer func() {
				if err != nil {
					log.LogErrorf(err.Error())
				}
			}()

			if err = ec.OpenStream(inode); err != nil {
				return
			}
			defer func() {
				ec.CloseStream(inode)
			}()
			if err = ec.RefreshExtentsCache(inode); err != nil {
				return
			}
			size, _, _ := ec.FileSize(inode)
			log.LogDebug("FileSize: " + strconv.FormatInt(int64(size), 10))

			pid, err := ecl.GetPartitionIdForWrite()
			if err != nil {
				return
			}
			log.LogDebug("PartitionId: " + strconv.FormatUint(pid, 10))
			//	DefaultStripeUnitSize = 64 * KB
			//	DefaultExtentFileSize = 4 * MB
			dataNum, parityNum, ecExtentFileSize, stripeUnitSize, err := ecl.GetPartitionInfo(pid)
			if err != nil {
				return
			}
			log.LogDebugf("PartitionInfo: dataNum(%v), parityNum(%v), ecExtentFileSize(%v), stripeUnitSize(%v)",
				dataNum, parityNum, ecExtentFileSize, stripeUnitSize)

			ech, err := echandler.NewEcHandler(int(stripeUnitSize), int(dataNum), int(parityNum))
			if err != nil {
				return
			}
			//the ec extent' is different with datanode extent. Max size of ec extent is 4M, Max size of datanode extent is 128M,
			extents, err := ecl.CreateExtentsForWrite(pid, uint64(size))
			if err != nil {
				return
			}

			if b, err := json.Marshal(extents); err == nil {
				log.LogDebug("CreateExtents: " + string(b))
			}

			outbufs := make([][]byte, dataNum+parityNum)
			for i := 0; i < int(dataNum+parityNum); i++ {
				outbufs[i] = make([]byte, ecExtentFileSize)
			}

			extentKeys = make([]proto.ExtentKey, len(extents))

			offset := 0
			for k, eid := range extents {
				var extentFileWriteSize int
				if size-offset < int(ecExtentFileSize*dataNum) {
					extentFileWriteSize = size - offset
				} else {
					extentFileWriteSize = int(ecExtentFileSize * dataNum)
				}
				//ecExtentWriteStripeNum defines actual stripe num to write on this extent
				ecExtentWriteStripeNum := uint32(extentFileWriteSize-1)/(stripeUnitSize*dataNum) + 1
				//initial the inbuf ecExtentWriteStripeNum times of (stripeUnitSize * dataNum), the spare bytes is set zero
				//thus every time we can write a total stripe to ecNode.
				inbuf := make([]byte, stripeUnitSize*dataNum*ecExtentWriteStripeNum)

				if extentFileWriteSize, err = ec.Read(inode, inbuf, offset, extentFileWriteSize); err != nil {
					return
				}

				for i := uint32(0); i < ecExtentWriteStripeNum; i++ {
					var shards [][]byte
					if shards, err = ech.Encode(inbuf[stripeUnitSize*dataNum*i : stripeUnitSize*dataNum*(i+1)]); err != nil {
						return
					}
					for j, shard := range shards {
						copy(outbufs[j][stripeUnitSize*i:stripeUnitSize*(i+1)], shard)
					}
				}
				if err = ecl.Write(outbufs, ecExtentWriteStripeNum, eid, pid); err != nil {
					return
				}

				extentKeys[k].FileOffset = uint64(offset)
				extentKeys[k].PartitionId = pid
				extentKeys[k].ExtentId = eid
				extentKeys[k].ExtentOffset = 0
				extentKeys[k].Size = uint32(extentFileWriteSize)

				offset += extentFileWriteSize
			}

			if b, err := json.Marshal(extentKeys); err == nil {
				log.LogDebug("UpdateExtents: " + string(b))
			}
			err = metaWrapper.UpdateExtentKeys(inode, extentKeys)
		}(i)
	}
}
