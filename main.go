package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/dolthub/swiss"
)

type Town struct {
	min   int64
	max   int64
	temp  int64
	count int64
}

func (town *Town) Update(temp int64) {
	town.temp += temp
	town.count++
	town.min = min(town.min, temp)
	town.max = max(town.max, temp)
}

func (town *Town) Sum(other *Town) {
	town.temp += other.temp
	town.count += other.count
	town.min = min(town.min, other.min)
	town.max = max(town.max, other.max)
}

type Block struct {
	buf   []byte
	len   int
	towns *swiss.Map[string, *Town]
}

const (
	TOWNS_COUNT = 413
)

var filename = flag.String("file", "measurements.txt", "file path to measurements")
var limit = flag.Int64("limit", 0, "for DEV purpose limit the amount to read from file")
var verbose = flag.Bool("verbose", false, "verbose")
var blockSize = os.Getpagesize() * 1000
var blockCount = 1000
var blocks chan *Block
var towns = swiss.NewMap[string, *Town](TOWNS_COUNT)
var townsLock = sync.Mutex{}
var infos []string

func oops(err error) {
	if err == nil {
		return
	}

	panic(err)
}

func scanBlock(b *Block) {
	offset := 3

	b.towns.Clear()

	for index := 0; index < b.len; index++ {
		var townName string
		var temp int64
		var minus bool

		startIndex := index
		index += offset

		for {
			if b.buf[index] == ';' {
				townName = string(b.buf[startIndex:index])
				index += 1

				break
			}

			index++
		}

	loop:
		for {
			switch b.buf[index] {
			case '.':
			case '-':
				minus = true
			case '\n':
				break loop
			default:
				temp = temp * 10
				temp += int64(b.buf[index] - '0')
			}

			index++
		}

		if minus {
			temp = temp * -1
		}

		town, ok := b.towns.Get(townName)
		if !ok {
			town = &Town{}

			b.towns.Put(townName, town)
		}

		town.Update(temp)
	}

	townsLock.Lock()
	b.towns.Iter(func(k string, v *Town) (stop bool) {
		town, ok := towns.Get(k)
		if !ok {
			town = &Town{}
			towns.Put(k, town)
		}

		town.Sum(v)

		return false
	})
	townsLock.Unlock()
}

func info(s string) {
	if *verbose {
		infos = append(infos, s)
	}
}

func readBlocks() {
	start := time.Now()
	defer func() {
		info(fmt.Sprintf("time read blocks: %v", time.Since(start)))
	}()

	file, err := os.Open(*filename)
	oops(err)

	defer func() {
		oops(file.Close())
	}()

	var read int64 = 0
	var remainder []byte

	blocksWg := sync.WaitGroup{}

loop:
	for {
		b := <-blocks

		if len(remainder) > 0 {
			copy(b.buf, remainder)
		}

		if *limit > 0 && read > *limit {
			break loop
		}

		n, err := file.Read(b.buf[len(remainder):])
		if err == io.EOF {
			break
		}
		oops(err)

		read += int64(n)

		l := len(remainder) + n

		var i int

		for i = l; i > 0 && b.buf[i-1] != '\n'; i-- {
		}

		b.len = i

		remainder = nil

		if i < l {
			remainder = make([]byte, l-i)

			copy(remainder, b.buf[i:])
		}

		blocksWg.Add(1)

		go func(b *Block) {
			defer func() {
				blocksWg.Done()

				blocks <- b
			}()

			scanBlock(b)
		}(b)
	}

	blocksWg.Wait()

	info(fmt.Sprintf("bytes read: %d", read))
}

func printMeasurements() {
	townNames := []string{}
	towns.Iter(func(k string, v *Town) bool {
		townNames = append(townNames, k)
		return false
	})

	sort.Strings(townNames)

	var count int64

	fmt.Printf("{\n")

	for i, townName := range townNames {
		if i > 0 {
			fmt.Printf(",")
		}

		town, _ := towns.Get(townName)
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", townName, float64(town.min)/10.0, float64(town.temp)/float64(town.count*10), float64(town.max)/10.0)
		count += town.count
	}

	fmt.Printf("}\n")

	info(fmt.Sprintf("count rows: %d", count))
}

func main() {
	flag.Parse()

	start := time.Now()
	defer func() {
		info(fmt.Sprintf("time needed: %v", time.Since(start)))

		if *verbose {
			for _, i := range infos {
				fmt.Println(i)
			}
		}
	}()

	if *limit > 0 {
		info(fmt.Sprintf("read limit: %v", *limit))
	}

	blocks = make(chan *Block, blockCount)

	for i := 0; i < blockCount; i++ {
		b := &Block{
			buf:   make([]byte, blockSize),
			towns: swiss.NewMap[string, *Town](TOWNS_COUNT),
		}

		blocks <- b
	}

	readBlocks()
	printMeasurements()
}
