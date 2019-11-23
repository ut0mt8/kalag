package lag

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

type OffsetStatus struct {
	Current int64
	End     int64
	Lag     int64
}

func GetOffsetTimestamp(consumer sarama.Consumer, topic string, partition int32, offset int64) (time.Time, error) {
        cp, err := consumer.ConsumePartition(topic, partition, offset)
        if nil != err {
                return time.Time{}, fmt.Errorf("cannot consumme partition: ", err)
        }

        select {
        case <-time.After(500 * time.Millisecond):
                return time.Time{}, fmt.Errorf("no message in topic")
        case consumerError := <-cp.Errors():
                return time.Time{}, fmt.Errorf("consumer error: ", consumerError.Err)
        case msg := <-cp.Messages():
                return msg.Timestamp, nil
        }

        return time.Time{}, fmt.Errorf("unknow error")
}

func GetLag(brokers string, topic string, group string) (map[int32]OffsetStatus, error) {

	ofs := make(map[int32]OffsetStatus)
	kcfg := sarama.NewConfig()
	kcfg.Consumer.Return.Errors = true
	kcfg.Version = sarama.V2_1_0_0

	bks := strings.Split(brokers, ",")

        cadmin, err := sarama.NewClusterAdmin(bks, kcfg)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to broker : %v", err)
	}


	topics, err := cadmin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("cannot list topics : %v", err)
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
		return nil, fmt.Errorf("cannot list groups : %v", err)
	}

	cfound := false
	for grp, _ := range groups {
		if grp == group {
			cfound = true
			break
		}
	}
	if !cfound {
		return nil, fmt.Errorf("consumergroup %s doesn't exist", topic)
	}
        cadmin.Close()

	client, err := sarama.NewClient(bks, kcfg)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to broker : %v", err)
	}

	parts, err := client.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("cannot get partitions : %v", err)
	}

	mng, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		return nil, fmt.Errorf("cannot create offset manager : %v", err)
	}

	for _, part := range parts {
		end, err := client.GetOffset(topic, part, sarama.OffsetNewest)
		if err != nil {
		        return nil, fmt.Errorf("cannot get partition offset : %v", err)
		}
		pmng, err := mng.ManagePartition(topic, part)
		if err != nil {
		        return nil, fmt.Errorf("cannot create partition manager : %v", err)
		}
		cur, _ := pmng.NextOffset()
		if err != nil {
		        return nil, fmt.Errorf("cannot get group offset : %v", err)
		}
		if cur == -1 {
			cur = 0
		}
                lag := end - cur
                // get ts of end
                // get ts of cur 
		ofs[part] = OffsetStatus{
			Current: cur,
			End:     end,
			Lag:     lag,
		}
	}
	mng.Close()
	client.Close()
	return ofs, nil
}
