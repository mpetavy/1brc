package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
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

	switch {
	case temp < m.min:
		m.min = temp
	case temp > m.max:
		m.max = temp
	}
}

func (m *Measurement) Calc(other Measurement) {
	m.temp += other.temp
	m.count += other.count

	switch {
	case other.min < m.min:
		m.min = other.min
	case other.max > m.max:
		m.max = other.max
	}
}

type Measurements map[string]Measurement

var filename = flag.String("file", "/home/ransom/java/1brc/measurements.txt", "file path to measurements")
var verbose = flag.Bool("v", false, "verbose")
var pageCount int64
var blockSize int64
var blockCh chan []byte
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

func scanBlock(b []byte) {
	offset := 3

	ms := make(Measurements)

	for index := 0; index < len(b); index++ {
		var town string
		var temp int64
		var minus bool

		startIndex := index
		index += offset

		for {
			if b[index] == ';' {
				town = string(b[startIndex:index])
				index += 1

				break
			}

			index++
		}

	loop:
		for {
			switch b[index] {
			case '.':
			case '-':
				minus = true
			case '\n':
				break loop
			default:
				temp = temp * 10
				temp += int64(b[index] - '0')
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

loop:
	for {
		b := make([]byte, blockSize)
		if len(remainder) > 0 {
			copy(b, remainder)
		}

		if limitRead > 0 && read > limitRead {
			break loop
		}

		n, err := file.Read(b[len(remainder):])
		if err == io.EOF {
			break
		}
		oops(err)

		read += int64(n)

		l := len(remainder) + n

		var i int

		for i = l; i > 0 && b[i-1] != '\n'; i-- {
		}

		remainder = nil

		if i < l {
			remainder = make([]byte, l-i)
			copy(remainder, b[i:])
		}

		blockCh <- b[:i]
	}

	close(blockCh)

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

	pageCount = 10
	blockSize = int64(os.Getpagesize()) * pageCount

	fi, err := os.Stat(*filename)
	oops(err)

	countBlocks := int64(math.Round(float64(fi.Size()) / float64(blockSize)))

	blockCh = make(chan []byte, countBlocks)

	go readBlocks()
	go readMeasurements()

	wgReader := sync.WaitGroup{}
	sem := make(chan struct{}, runtime.NumCPU())

	for b := range blockCh {
		sem <- struct{}{}

		wgReader.Add(1)

		go func(b []byte) {
			defer func() {
				wgReader.Done()
				<-sem
			}()

			scanBlock(b)
		}(b)
	}

	wgReader.Wait()

	close(measurements)

	<-done
}
