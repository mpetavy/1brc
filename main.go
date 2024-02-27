package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type block struct {
	start int64
	end   int64
}

type measurement struct {
	town string
	temp int
}

type stats struct {
	lenMin  int
	lenMax  int
	tempMin int
	tempMax int
}

var filename = "/home/ransom/java/1brc/measurements.txt"
var buf []byte
var pageCount int64
var blockSize int64
var blockCh chan block
var mu = sync.Mutex{}

var debug int64 = 0

var measurements = []measurement{}

func oops(err error) {
	if err == nil {
		return
	}

	panic(err)
}

func printBytes(ba []byte, breakOnLineEndings bool) {
	sb := strings.Builder{}

	for _, r := range string(ba) {
		if strconv.IsPrint(r) {
			sb.WriteString(string(r))
		} else {
			sb.WriteString("\\x")
			sb.WriteString(hex.EncodeToString([]byte(string(r))))
		}
	}

	str := sb.String()

	if breakOnLineEndings {
		endings := []string{
			"\\x0d\\x0a",
			"\\x0d",
			"\\x0a",
		}

		for _, ending := range endings {
			if strings.Contains(str, ending) {
				str = strings.ReplaceAll(str, ending, ending+"\n")

				break
			}
		}
	}

	fmt.Printf("%s\n", str)
}

func scanBlock(start int64, end int64) {
	offset := int64(3)

	for index := start; index <= end; index++ {
		var town string
		var temp int
		var minus bool

		startIndex := index
		index += offset

		for {
			if buf[index] == ';' {
				town = string(buf[startIndex:index])
				index += 1

				break
			}

			index++
		}

	loop:
		for {
			switch buf[index] {
			case '.':
			case '-':
				minus = true
			case '\n':
				break loop
			default:
				temp = temp * 10
				temp += int(buf[index] - '0')
			}

			index++
		}

		if minus {
			temp = temp * -1
		}

		m := measurement{
			town: town,
			temp: temp,
		}

		mu.Lock()
		measurements = append(measurements, m)
		mu.Unlock()
	}
}

func readBlocks() {
	start := time.Now()
	defer func() {
		fmt.Printf("time readBlocks: %v\n", time.Since(start))
	}()

	file, err := os.Open(filename)
	oops(err)

	defer func() {
		oops(file.Close())
	}()

	var bufStart int64
	var blockStart int64
	var blockEnd int64
	buflen := int64(len(buf))

	var i int64 = 0

loop:
	for {
		bufEnd := bufStart + blockSize
		if bufEnd > buflen {
			bufEnd = buflen
		}

		if bufStart == bufEnd {
			break loop
		}

		if debug > 0 && i == debug {
			break loop
		}

		i++

		n, err := file.Read(buf[bufStart:bufEnd])
		oops(err)

		blockEnd = bufStart + int64(n) - 1
		for ; blockEnd >= blockStart && buf[blockEnd] != '\n'; blockEnd-- {
		}

		blockCh <- block{
			start: blockStart,
			end:   blockEnd,
		}

		blockStart = blockEnd + 1

		bufStart += int64(n)
	}

	close(blockCh)

	fmt.Printf("bytes read:  %d\n", bufStart)
}

func readMeasurements() stats {
	s := stats{
		lenMin:  100,
		lenMax:  0,
		tempMin: 1000,
		tempMax: 0,
	}

	for _, m := range measurements {
		l := len(m.town)
		s.lenMin = min(s.lenMin, l)
		s.lenMax = max(s.lenMax, l)

		s.tempMin = min(s.tempMin, m.temp)
		s.tempMax = max(s.tempMax, m.temp)
	}

	return s
}

func main() {
	start := time.Now()
	defer func() {
		fmt.Printf("time main: %v\n", time.Since(start))
	}()

	if debug > 0 {
		fmt.Printf("debug limit: %v\n", debug)
	}

	pageCount = 100
	blockSize = int64(os.Getpagesize()) * pageCount

	fi, err := os.Stat(filename)
	oops(err)

	countBlocks := int(math.Round(float64(fi.Size()) / float64(blockSize)))

	buf = make([]byte, fi.Size())
	blockCh = make(chan block, countBlocks)

	go readBlocks()

	wgReader := sync.WaitGroup{}

	for b := range blockCh {
		wgReader.Add(1)

		go func(start int64, end int64) {
			defer wgReader.Done()

			scanBlock(start, end)
		}(b.start, b.end)
	}

	wgReader.Wait()

	sum := readMeasurements()

	//fmt.Printf("max go routines used: %d\n", mg)
	fmt.Printf("town len min: %d\n", sum.lenMin)
	fmt.Printf("town len max: %d\n", sum.lenMax)
	fmt.Printf("temp min: %d\n", sum.tempMin)
	fmt.Printf("temp max: %d\n", sum.tempMax)
}
