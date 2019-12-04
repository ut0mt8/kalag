package lag

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

type OffsetStatus struct {
	Leader    int32
	Current   int64
	End       int64
	OffsetLag int64
	TimeLag   time.Duration
}

var nullTime time.Time = time.Time{}

func GetGroupOffset(broker *sarama.Broker, topic string, partition int32, group string) (int64, error) {
	request := &sarama.OffsetFetchRequest{ Version:4, ConsumerGroup: group }
	request.AddPartition(topic, partition)

	fr, err := broker.FetchOffset(request)
        if err != nil {
            return 0, fmt.Errorf("cannot fetch offset request: %v", err)
        }

        block := fr.GetBlock(topic, partition)
        if block == nil {
            return 0, fmt.Errorf("cannot get block records")
        }

        return block.Offset, nil
}

func GetTimestamp(broker *sarama.Broker, topic string, partition int32, offset int64) (time.Time, error) {
	request := &sarama.FetchRequest{ Version:4 }
	request.AddBlock(topic, partition, offset, 1)

	fr, err := broker.Fetch(request)
        if err != nil {
            return nullTime, fmt.Errorf("cannot fetch request: %v", err)
        }

        block := fr.GetBlock(topic, partition)
        if block == nil || block.Records == nil {
            return nullTime, fmt.Errorf("cannot get block records")
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
		return nil, fmt.Errorf("cannot connect to broker: %v", err)
	}

	topics, err := cadmin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("cannot list topics: %v", err)
	}

	tfound := false
	for t, _ := range topics {
		if t == topic {
			tfound = true
			break
		}
	}
	if !tfound {
		return nil, fmt.Errorf("topic %s doesn't exist", topic)
	}

	groups, err := cadmin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("cannot list groups: %v", err)
	}

	cfound := false
	for grp, _ := range groups {
		if grp == group {
			cfound = true
			break
		}
	}
	if !cfound {
		return nil, fmt.Errorf("consumergroup %s doesn't exist", group)
	}
	cadmin.Close()

	client, err := sarama.NewClient(bks, cfg)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to broker: %v", err)
	}

	parts, err := client.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("cannot get partitions: %v", err)
	}

	for _, part := range parts {
		var tlag time.Duration

		leader, err := client.Leader(topic, part)
		if err != nil {
			return nil, fmt.Errorf("cannot get leader: %v", err)
		}

		end, err := client.GetOffset(topic, part, sarama.OffsetNewest)
		if err != nil {
			return nil, fmt.Errorf("cannot get partition last offset: %v", err)
		}
		if end < 1 {
			end = 0
		} else {
			end = end - 1
		}

                cur, err := GetGroupOffset(leader, topic, part, group)
		if err != nil {
			return nil, fmt.Errorf("cannot get group offset: %v", err)
		}
		if cur < 1 {
			cur = 0
		} else {
			cur = cur - 1
		}

		olag := end - cur

		if olag != 0 {
                        endTime, err := GetTimestamp(leader, topic, part, end)
			if err != nil {
				return nil, fmt.Errorf("cannot get end time: %v", err)
			}

                        curTime, err := GetTimestamp(leader, topic, part, cur)
			if err != nil {
				return nil, fmt.Errorf("cannot get current time: %v", err)
			}

			if curTime != nullTime && endTime != nullTime && endTime.After(curTime) {
				tlag = endTime.Sub(curTime)
			} else {
				tlag = 0
			}
		}

		ofs[part] = OffsetStatus{
			Leader:    leader.ID(),
			Current:   cur,
			End:       end,
			OffsetLag: olag,
			TimeLag:   tlag,
		}
	}
	return ofs, nil
}
