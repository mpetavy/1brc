package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type block []byte

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
var pageCount int64
var blockSize int64
var blockCh chan block

// var debug int64 = 1 * 1024 * 1024 * 1024
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

func scanBlock(b block) {
	offset := 3

	ms := []measurement{}

	for index := 0; index < len(b); index++ {
		var town string
		var temp int
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
				temp += int(b[index] - '0')
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

		ms = append(ms, m)
	}

	//mu.Lock()
	//measurements = append(measurements, ms...)
	//mu.Unlock()
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

	var read int64 = 0

	var remainder []byte

loop:
	for {
		b := make(block, blockSize)
		if len(remainder) > 0 {
			copy(b, remainder)
		}

		if debug > 0 && read > debug {
			break loop
		}

		n, err := file.Read(b[len(remainder):])
		if err == io.EOF {
			break
		}
		oops(err)

		read += int64(n)

		var i int

		for i = n; i > 0 && b[i-1] != '\n'; i-- {
		}

		remainder = nil

		if i < n {
			remainder = make([]byte, n-i)
			copy(remainder, b[i:])
		}

		blockCh <- b[:i]
	}

	close(blockCh)

	fmt.Printf("bytes read:  %d\n", read)
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

	pageCount = 10
	blockSize = int64(os.Getpagesize()) * pageCount

	fi, err := os.Stat(filename)
	oops(err)

	countBlocks := int(math.Round(float64(fi.Size()) / float64(blockSize)))

	blockCh = make(chan block, countBlocks)

	go readBlocks()

	wgReader := sync.WaitGroup{}

	for b := range blockCh {
		wgReader.Add(1)

		go func(b []byte) {
			defer wgReader.Done()

			scanBlock(b)
		}(b)
	}

	wgReader.Wait()

	sum := readMeasurements()

	//fmt.Printf("max go routines used: %d\n", mg)
	fmt.Printf("town len min: %d\n", sum.lenMin)
	fmt.Printf("town len max: %d\n", sum.lenMax)
	fmt.Printf("temp min: %d\n", sum.tempMin)
	fmt.Printf("temp max: %d\n", sum.tempMax)
}
