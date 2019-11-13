package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/namsral/flag"
        "os"
	"strings"
)

type Config struct {
	brokers string
	topic   string
	group   string
}

type OffsetStatus struct {
	current int64
	end     int64
	lag     int64
}

var config Config

func getLag(brokers string, topic string, group string) (map[int32]OffsetStatus, error) {

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
	for group, _ := range groups {
		if group == config.group {
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
		ofs[part] = OffsetStatus{
			current: cur,
			end:     end,
			lag:     end - cur,
		}
	}
	mng.Close()
	client.Close()
	return ofs, nil
}

func init() {
	flag.StringVar(&config.brokers, "brokers", "localhost:9092", "brokers to connect on")
	flag.StringVar(&config.topic, "topic", "", "topic to check")
	flag.StringVar(&config.group, "group", "", "group to check")
}

func main() {
        var p int32
	ofs := make(map[int32]OffsetStatus)

	flag.Parse()
	if config.topic == "" || config.group == "" {
		fmt.Printf("-topic and -group options are required\n")
                os.Exit(1)
	}

	ofs, err := getLag(config.brokers, config.topic, config.group)
	if err != nil {
		fmt.Printf("getLag failed : %v\n", err)
                os.Exit(1)
	}

	fmt.Printf("part\tend\tcurrent\tlag\n")
        for p = 0; p < int32(len(ofs)); p++ {
		fmt.Printf("%d\t%d\t%d\t%d\n", p, ofs[p].end, ofs[p].current, ofs[p].lag)
	}
}
