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

func GetOffsetTimestamp(brokers []string, cfg *sarama.Config, topic string, partition int32, offset int64) (time.Time, error) {

	c, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot create consumer: %v", err)
	}
	defer c.Close()

	cp, err := c.ConsumePartition(topic, partition, offset)
	if nil != err {
		return time.Time{}, fmt.Errorf("cannot consumme partition: ", err)
	}
	defer cp.Close()

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

func GetLagPartition(brokers []string, cfg *sarama.Config, topic string, group string, part int32) (OffsetStatus, error) {
	var tlag time.Duration

	c, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return OffsetStatus{}, fmt.Errorf("cannot connect to broker: %v", err)
	}

	leader, err := c.Leader(topic, part)
	if err != nil {
		return OffsetStatus{}, fmt.Errorf("cannot get leader: %v", err)
	}

	mng, err := sarama.NewOffsetManagerFromClient(group, c)
	if err != nil {
		return OffsetStatus{}, fmt.Errorf("cannot create offset manager: %v", err)
	}

	pmng, err := mng.ManagePartition(topic, part)
	if err != nil {
		return OffsetStatus{}, fmt.Errorf("cannot create partition manager: %v", err)
	}

	cur, _ := pmng.NextOffset()
	if err != nil {
		return OffsetStatus{}, fmt.Errorf("cannot get consumergroup current offset: %v", err)
	}
	if cur == -1 {
		cur = 0
	}

	end, err := c.GetOffset(topic, part, sarama.OffsetNewest)
	if err != nil {
		return OffsetStatus{}, fmt.Errorf("cannot get partition newest offset: %v", err)
	}

	mng.Close()
	c.Close()

	olag := end - cur

	if olag != 0 {

		endTime, err := GetOffsetTimestamp(brokers, cfg, topic, part, end-1)
		if err != nil {
			return OffsetStatus{}, fmt.Errorf("cannot get end time: %v", err)
		}

		curTime, err := GetOffsetTimestamp(brokers, cfg, topic, part, cur)
		if err != nil {
			return OffsetStatus{}, fmt.Errorf("cannot get current time: %v", err)
		}

		tlag = endTime.Sub(curTime)
		if tlag < 0 {
			tlag = 0
		}
	}

	return OffsetStatus{
		Leader:    leader.ID(),
		Current:   cur,
		End:       end,
		OffsetLag: olag,
		TimeLag:   tlag,
	}, nil
}

func GetLag(brokers string, topic string, group string) (map[int32]OffsetStatus, error) {

	ofs := make(map[int32]OffsetStatus)
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.AutoCommit.Enable = false

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

	client.Close()

	for _, part := range parts {
		ofs[part], err = GetLagPartition(bks, cfg, topic, group, part)
		if err != nil {
			return nil, fmt.Errorf("cannot get lag partition: %v", err)
		}
	}

	return ofs, nil
}
