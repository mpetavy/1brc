package main

import (
	"flag"
	"fmt"
	"github.com/dolthub/swiss"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Town struct {
	town  string
	min   int64
	max   int64
	temp  int64
	count int64
	sync.Mutex
}

func (town *Town) Update(temp int64) {
	town.Lock()
	defer town.Unlock()

	town.temp += temp
	town.count++
	town.min = min(town.min, temp)
	town.max = max(town.max, temp)
}

type Block struct {
	buf []byte
	len int
}

const (
	TOWNS_COUNT = 413
)

var filename = flag.String("file", "/home/ransom/java/1brc/measurements.txt", "file path to measurements")
var verbose = flag.Bool("v", false, "verbose")
var blockSize = os.Getpagesize() * 10
var blockCount = 1000
var blocks chan *Block
var towns = swiss.NewMap[string, *Town](TOWNS_COUNT)
var townsLock = sync.RWMutex{}
var allTowns atomic.Bool

//var limitRead int64 = 1 * 1024 * 1024 * 1024

var limitRead int64 = 0

func oops(err error) {
	if err == nil {
		return
	}

	panic(err)
}

func scanBlock(b *Block) {
	offset := 3

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

		if allTowns.Load() {
			town, _ := towns.Get(townName)

			town.Update(temp)
		} else {
			townsLock.RLock()
			town, ok := towns.Get(townName)
			townsLock.RUnlock()

			if !ok {
				townsLock.Lock()

				town, ok = towns.Get(townName)
				if !ok {
					town = &Town{}

					towns.Put(townName, town)

					if towns.Count() == TOWNS_COUNT {
						allTowns.Store(true)
					}
				}

				townsLock.Unlock()
			}

			town.Update(temp)
		}
	}
}

func readBlocks() {
	start := time.Now()
	defer func() {
		if *verbose {
			log.Printf("time readBlocks: %v\n", time.Since(start))
		}
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

		if limitRead > 0 && read > limitRead {
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

	if *verbose {
		log.Printf("bytes read:  %d\n", read)
	}
}

func readMeasurements() {
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

	if *verbose {
		fmt.Printf("count: %d\n", count)
	}
}

func main() {
	flag.Parse()

	start := time.Now()
	defer func() {
		if *verbose {
			log.Printf("time main: %v\n", time.Since(start))
		}
	}()

	if limitRead > 0 {
		if *verbose {
			log.Printf("debug limit: %v\n", limitRead)
		}
	}

	blocks = make(chan *Block, blockCount)

	for i := 0; i < blockCount; i++ {
		b := &Block{
			buf: make([]byte, blockSize),
		}

		blocks <- b
	}

	readBlocks()
	readMeasurements()
}
