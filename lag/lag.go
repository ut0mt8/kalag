package lag

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

type lagError struct {
	topic     string
	group     string
	operation string
	err       error
}

type lagPartitionError struct {
	topic     string
	group     string
	partition int32
	operation string
	err       error
}

func (e *lagError) Error() string {
	return fmt.Sprintf("getLag(topic: %s, group: %s) failed during [%s]: %v", e.topic, e.group, e.operation, e.err)
}

func (e *lagPartitionError) Error() string {
	return fmt.Sprintf("getLag(topic: %s, group: %s, partition: %d) failed during [%s]: %v", e.topic, e.group, e.partition, e.operation, e.err)
}

type OffsetStatus struct {
	Leader         int32
	Replicas       int
	InSyncReplicas int
	Current        int64
	Newest         int64
	Oldest         int64
	MsgNumber      int64
	OffsetLag      int64
	OldestTime     time.Duration
	TimeLag        time.Duration
}

var nullTime time.Time = time.Time{}

func GetGroupOffset(broker *sarama.Broker, topic string, partition int32, group string) (int64, error) {
	request := &sarama.OffsetFetchRequest{Version: 4, ConsumerGroup: group}
	request.AddPartition(topic, partition)

	fr, err := broker.FetchOffset(request)
	if err != nil {
		return 0, fmt.Errorf("cannot fetch offset request: %v", err)
	}

	block := fr.GetBlock(topic, partition)
	if block == nil {
		return 0, fmt.Errorf("cannot get block")
	}

	return block.Offset, nil
}

func GetTimestamp(broker *sarama.Broker, topic string, partition int32, offset int64) (time.Time, error) {
	request := &sarama.FetchRequest{Version: 4}
	request.AddBlock(topic, partition, offset, 1)

	fr, err := broker.Fetch(request)
	if err != nil {
		return nullTime, fmt.Errorf("cannot fetch request: %v", err)
	}

	block := fr.GetBlock(topic, partition)
	if block == nil || block.Records == nil {
		return nullTime, fmt.Errorf("cannot get block")
	}

	return block.Records.RecordBatch.MaxTimestamp, nil
}

func GetLag(brokers string, topic string, group string) (map[int32]OffsetStatus, error) {
	ofs := make(map[int32]OffsetStatus)
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0

	bks := strings.Split(brokers, ",")

	cadmin, err := sarama.NewClusterAdmin(bks, cfg)
	if err != nil {
		return nil, &lagError{topic, group, "admin-connect", err}
	}

	topics, err := cadmin.ListTopics()
	if err != nil {
		return nil, &lagError{topic, group, "list-topics", err}
	}

	tfound := false
	for t := range topics {
		if t == topic {
			tfound = true
			break
		}
	}
	if !tfound {
		return nil, &lagError{topic, group, "list-topics", fmt.Errorf("topic not found")}
	}

	groups, err := cadmin.ListConsumerGroups()
	if err != nil {
		return nil, &lagError{topic, group, "list-groups", err}
	}

	cfound := false
	for grp := range groups {
		if grp == group {
			cfound = true
			break
		}
	}
	if !cfound {
		return nil, &lagError{topic, group, "list-groups", fmt.Errorf("group not found")}
	}
	cadmin.Close()

	client, err := sarama.NewClient(bks, cfg)
	if err != nil {
		return nil, &lagError{topic, group, "client-connect", err}
	}
	defer client.Close()

	parts, err := client.Partitions(topic)
	if err != nil {
		return nil, &lagError{topic, group, "list-partitions", err}
	}

	for _, part := range parts {
		replicas, err := client.Replicas(topic, part)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-replicas", err}
		}

		isr, err := client.InSyncReplicas(topic, part)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-insync-replicas", err}
		}

		leader, err := client.Leader(topic, part)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-leader", err}
		}
		if ok, _ := leader.Connected(); !ok {
			leader.Open(client.Config())
		}

		var current, newest, oldest, msgnumber, olag int64 = -1, -1, -1, 0, 0

		newest, err = client.GetOffset(topic, part, sarama.OffsetNewest)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-topic-newest-offset", err}
		}

		/* newest is the next offset on the partition, if at 0 the partition had never be filled,
		   so there are never been data, and no oldest offest */
		if newest > 0 {
			oldest, err = client.GetOffset(topic, part, sarama.OffsetOldest)
			if err != nil {
				return nil, &lagPartitionError{topic, group, part, "get-topic-oldest-offset", err}
			}
		}

		/* oldest is the last offset availabble on the partition, so the number of message is the
		   difference between newest and oldest if any */
		if newest > 0 && oldest >= 0 {
			msgnumber = newest - oldest
		}

		var oldestTime, newestTime time.Time
		var tlag, oldestInterval time.Duration

		/* if there are no message, we cannot get timestamps */
		if msgnumber > 0 {
			/* newest is the next offset so getting the previous one */
			newestTime, err = GetTimestamp(leader, topic, part, newest-1)
			if err != nil {
				return nil, &lagPartitionError{topic, group, part, "get-timestamp-topic-offset-latest", err}
			}

			oldestTime, err = GetTimestamp(leader, topic, part, oldest)
			if err != nil {
				return nil, &lagPartitionError{topic, group, part, "get-timestamp-topic-offset-oldest", err}
			}
			oldestInterval = time.Now().Sub(oldestTime)
		}

		/* for group offset we query the coordinator of the group */
		coordinator, err := client.Coordinator(group)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-group-coordinator", err}
		}
		if ok, _ := coordinator.Connected(); !ok {
			coordinator.Open(client.Config())
		}

		current, err = GetGroupOffset(coordinator, topic, part, group)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-group-offset", err}
		}

		/* offset lag is only defined if there are message */
		if newest >= 0 && current >= 0 {
			olag = newest - current
		}

		/* we need at least two messages available */
		if msgnumber > 1 && olag > 0 && current > oldest {
			currentTime, err := GetTimestamp(leader, topic, part, current-1)
			if err != nil {
				return nil, &lagPartitionError{topic, group, part, "get-timestamp-group-offset", err}
			}

			if currentTime != nullTime && newestTime != nullTime && newestTime.After(currentTime) {
				tlag = newestTime.Sub(currentTime)
			} else {
				tlag = 0
			}
		}

		ofs[part] = OffsetStatus{
			Leader:         leader.ID(),
			Replicas:       len(replicas),
			InSyncReplicas: len(isr),
			Current:        current,
			Newest:         newest,
			Oldest:         oldest,
			MsgNumber:      msgnumber,
			OldestTime:     oldestInterval,
			OffsetLag:      olag,
			TimeLag:        tlag,
		}
	}
	return ofs, nil
}
