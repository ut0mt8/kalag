package main

import (
	"fmt"
	"github.com/namsral/flag"
	"github.com/ut0mt8/kalag/lag"
	"os"
)

type config struct {
	brokers string
	topic   string
	group   string
}

var conf config

func init() {
	flag.StringVar(&conf.brokers, "brokers", "localhost:9092", "brokers to connect on")
	flag.StringVar(&conf.topic, "topic", "", "topic to check")
	flag.StringVar(&conf.group, "group", "", "group to check")
}

func main() {
	var p int32

	flag.Parse()
	if conf.topic == "" || conf.group == "" {
		fmt.Printf("-topic and -group options are required\n")
		os.Exit(1)
	}

	ofs, err := lag.GetLag(conf.brokers, conf.topic, conf.group)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	fmt.Printf("part\tleader\trepl\tisr\tmsgnb\toldest\tnewest\tcurrent\tolag\ttlag\toldestts\n")
	for p = 0; p < int32(len(ofs)); p++ {
		fmt.Printf("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%v\t%v\n",
			p, ofs[p].Leader, ofs[p].Replicas, ofs[p].InSyncReplicas,
			ofs[p].MsgNumber, ofs[p].Oldest, ofs[p].Newest, ofs[p].Current,
			ofs[p].OffsetLag, ofs[p].TimeLag, ofs[p].OldestTime)
	}
}
