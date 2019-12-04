package lag

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"sync"
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

func GetOffsetTimestamp(brokers []string, cfg *sarama.Config, topic string, partition int32, offset int64) (time.Time, error) {

	c, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot create consumer: %v", err)
	}
	defer c.Close()

	pc, err := c.ConsumePartition(topic, partition, offset)
	if nil != err {
		return time.Time{}, fmt.Errorf("cannot consumme partition: ", err)
	}
	defer pc.AsyncClose()

	select {
	case <-time.After(1000 * time.Millisecond):
		fmt.Printf("x %d %d timeout \n", partition, offset)
		return nullTime, fmt.Errorf("no message in topic")
	case consumerError := <-pc.Errors():
		fmt.Printf("x %d %d %v \n", partition, offset, consumerError.Err)
		return nullTime, fmt.Errorf("consumer error: ", consumerError.Err)
	case msg := <-pc.Messages():
		fmt.Printf("x %d %d %v \n", partition, offset, msg.Timestamp)
		return msg.Timestamp, nil
	}

	return time.Time{}, fmt.Errorf("unknow error")
}

func GetLag(brokers string, topic string, group string) (map[int32]OffsetStatus, error) {

	var lock = sync.RWMutex{}
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

	plof := func(bks []string, cfg *sarama.Config, topic string, part int32, group string, wg *sync.WaitGroup) error {

		var tlag time.Duration
		fmt.Printf("started part %d\n", part)

		defer wg.Done()

		c, err := sarama.NewClient(bks, cfg)
		if err != nil {
			//return nil, fmt.Errorf("cannot connect to broker: %v", err)
			fmt.Printf("cannot connect to broker: %v", err)
		}

		leader, err := c.Leader(topic, part)
		if err != nil {
			//return nil, fmt.Errorf("cannot create offset manager: %v", err)
			fmt.Printf("cannot get leader: %v", err)
		}

		mng, err := sarama.NewOffsetManagerFromClient(group, c)
		if err != nil {
			//return nil, fmt.Errorf("cannot create offset manager: %v", err)
			fmt.Printf("cannot create offset manager: %v", err)
		}

		pmng, err := mng.ManagePartition(topic, part)
		if err != nil {
			//return nil, fmt.Errorf("cannot create partition manager: %v", err)
			fmt.Printf("cannot create partition manager: %v", err)
		}

		cur, _ := pmng.NextOffset()
		if err != nil {
			//return nil, fmt.Errorf("cannot get consumergroup current offset: %v", err)
			fmt.Printf("cannot get consumergroup current offset: %v", err)
		}
		if cur < 1 {
			cur = 0
		} else {
			cur = cur - 1
		}

		pmng.AsyncClose()
		mng.Close()

		end, err := c.GetOffset(topic, part, sarama.OffsetNewest)
		if err != nil {
			//return nil, fmt.Errorf("cannot get partition newest offset: %v", err)
			fmt.Printf("cannot get partition newest offset: %v", err)
		}
		if end < 1 {
			end = 0
		} else {
			end = end - 1
		}

		c.Close()
		olag := end - cur

		if olag != 0 {

			curTime, err := GetOffsetTimestamp(bks, cfg, topic, part, cur)
			if err != nil {
				//return nil, fmt.Errorf("cannot get current time: %v", err)
				fmt.Errorf("cannot get current time: %v", err)
			}

			endTime, err := GetOffsetTimestamp(bks, cfg, topic, part, end)
			if err != nil {
				//return nil, fmt.Errorf("cannot get end time: %v", err)
				fmt.Errorf("cannot get end time: %v", err)
			}

			if curTime != nullTime && endTime != nullTime && endTime.After(curTime) {
				tlag = endTime.Sub(curTime)
			} else {
				tlag = 0
			}
		}

		lock.Lock()
		ofs[part] = OffsetStatus{
			Leader:    leader.ID(),
			Current:   cur,
			End:       end,
			OffsetLag: olag,
			TimeLag:   tlag,
		}
		lock.Unlock()

		fmt.Printf("ended part %d\n", part)
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(parts))
	for _, part := range parts {
		go plof(bks, cfg, topic, part, group, &wg)
	}
	wg.Wait()
	return ofs, nil
}

