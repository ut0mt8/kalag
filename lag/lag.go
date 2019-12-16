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
	Leader    int32
	Current   int64
	Latest    int64
	OffsetLag int64
	TimeLag   time.Duration
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
	for t, _ := range topics {
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
	for grp, _ := range groups {
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
		var tlag time.Duration

		leader, err := client.Leader(topic, part)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-leader", err}
		}
		if ok, _ := leader.Connected(); !ok {
			leader.Open(client.Config())
		}

		last, err := client.GetOffset(topic, part, sarama.OffsetNewest)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-topic-newest-offset", err}
		}
		if last < 1 {
			last = 0
		} else {
			last = last - 1
		}

		coordinator, err := client.Coordinator(group)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-group-coordinator", err}
		}
		if ok, _ := coordinator.Connected(); !ok {
			coordinator.Open(client.Config())
		}

		cur, err := GetGroupOffset(coordinator, topic, part, group)
		if err != nil {
			return nil, &lagPartitionError{topic, group, part, "get-group-offset", err}
		}
		if cur < 1 {
			cur = 0
		} else {
			cur = cur - 1
		}

		olag := last - cur

		if olag != 0 {
			lastTime, err := GetTimestamp(leader, topic, part, last)
			if err != nil {
				return nil, &lagPartitionError{topic, group, part, "get-timestamp-topic-offset", err}
			}

			curTime, err := GetTimestamp(leader, topic, part, cur)
			if err != nil {
				return nil, &lagPartitionError{topic, group, part, "get-timestamp-group-offset", err}
			}

			if curTime != nullTime && lastTime != nullTime && lastTime.After(curTime) {
				tlag = lastTime.Sub(curTime)
			} else {
				tlag = 0
			}
		}

		ofs[part] = OffsetStatus{
			Leader:    leader.ID(),
			Current:   cur,
			Latest:    last,
			OffsetLag: olag,
			TimeLag:   tlag,
		}
	}
	return ofs, nil
}
