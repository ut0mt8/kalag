package main

import (
	"fmt"
	"github.com/namsral/flag"
	"github.com/ut0mt8/kalag/lag"
	"os"
)

type Config struct {
	brokers string
	topic   string
	group   string
}

var config Config

func init() {
	flag.StringVar(&config.brokers, "brokers", "localhost:9092", "brokers to connect on")
	flag.StringVar(&config.topic, "topic", "", "topic to check")
	flag.StringVar(&config.group, "group", "", "group to check")
}

func main() {
	var p int32

	flag.Parse()
	if config.topic == "" || config.group == "" {
		fmt.Printf("-topic and -group options are required\n")
		os.Exit(1)
	}

	ofs, err := lag.GetLag(config.brokers, config.topic, config.group)
	if err != nil {
		fmt.Printf("kalag failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("part\tleader\tlast\tcurrent\tolag\ttlag\n")
	for p = 0; p < int32(len(ofs)); p++ {
		fmt.Printf("%d\t%d\t%d\t%d\t%d\t%v\n", p, ofs[p].Leader, ofs[p].Latest, ofs[p].Current, ofs[p].OffsetLag, ofs[p].TimeLag)
	}
}
