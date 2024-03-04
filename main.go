package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Measurement struct {
	town  string
	min   int64
	max   int64
	temp  int64
	count int64
}

func (m *Measurement) Update(temp int64) {
	m.temp += temp
	m.count++
	m.min = min(m.min, temp)
	m.max = max(m.max, temp)
}

func (m *Measurement) Calc(other Measurement) {
	m.temp += other.temp
	m.count += other.count
	m.min = min(m.min, other.min)
	m.max = max(m.max, other.max)
}

type Block struct {
	buf []byte
	len int
}

type Measurements map[string]Measurement

var filename = flag.String("file", "/home/ransom/java/1brc/measurements.txt", "file path to measurements")
var verbose = flag.Bool("v", false, "verbose")
var workerCount = runtime.NumCPU()
var blockSize = os.Getpagesize() * 10
var blockCount = 100
var blocks chan Block
var done = make(chan struct{})
var measurements = make(chan Measurements, 1000000)

// var limitRead int64 = 1 * 1024 * 1024 * 1024
var limitRead int64 = 0

func oops(err error) {
	if err == nil {
		return
	}

	panic(err)
}

func scanBlock(b Block) {
	offset := 3

	ms := make(Measurements)

	for index := 0; index < b.len; index++ {
		var town string
		var temp int64
		var minus bool

		startIndex := index
		index += offset

		for {
			if b.buf[index] == ';' {
				town = string(b.buf[startIndex:index])
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

		m, _ := ms[town]
		m.town = town
		m.Update(temp)
		ms[town] = m
	}

	measurements <- ms
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

	workerWg := sync.WaitGroup{}
	workerSem := make(chan struct{}, workerCount)
	for i := 0; i < workerCount; i++ {
		workerSem <- struct{}{}
	}

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

		go func() {
			<-workerSem

			workerWg.Add(1)
			
			go func(b Block) {
				defer func() {
					workerWg.Done()
				}()

				scanBlock(b)

				blocks <- b

				workerSem <- struct{}{}
			}(b)
		}()
	}

	workerWg.Wait()

	close(measurements)

	if *verbose {
		log.Printf("bytes read:  %d\n", read)
	}
}

func readMeasurements() {
	sum := make(Measurements)

	for ms := range measurements {
		for town, m := range ms {
			s, _ := sum[town]
			s.town = town
			s.Calc(m)
			sum[town] = s
		}
	}

	towns := []string{}
	for town := range sum {
		towns = append(towns, town)
	}

	sort.Strings(towns)

	var count int64

	fmt.Printf("{\n")
	for i, town := range towns {
		if i > 0 {
			fmt.Printf(",")
		}
		s := sum[town]
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", town, float64(s.min)/10.0, float64(s.temp)/float64(s.count*10), float64(s.max)/10.0)
		count += s.count
	}
	fmt.Printf("}\n")

	if *verbose {
		fmt.Printf("count: %d\n", count)
	}

	close(done)
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

	blocks = make(chan Block, blockCount)

	for i := 0; i < blockCount; i++ {
		b := Block{
			buf: make([]byte, blockSize),
		}

		blocks <- b
	}

	go readBlocks()
	go readMeasurements()

	<-done
}
