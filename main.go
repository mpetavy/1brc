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
var blockCh chan []byte
var done = make(chan struct{})
var measurements = make(chan []measurement, 10000)

//var debug int64 = 1 * 1024 * 1024 * 1024

var debug int64 = 0

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

func scanBlock(b []byte) {
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

	measurements <- ms
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
		b := make([]byte, blockSize)
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

	fmt.Printf("bytes read:  %d\n", read)
}

func readMeasurements() {
	s := stats{
		lenMin:  100,
		lenMax:  0,
		tempMin: 1000,
		tempMax: 0,
	}

	count := 0

	for ms := range measurements {
		count += len(ms)

		for i := 0; i < len(ms); i++ {
			m := ms[i]

			l := len(m.town)
			s.lenMin = min(s.lenMin, l)
			s.lenMax = max(s.lenMax, l)

			s.tempMin = min(s.tempMin, m.temp)
			s.tempMax = max(s.tempMax, m.temp)
		}
	}

	fmt.Printf("count: %d\n", count)
	fmt.Printf("town len min: %d\n", s.lenMin)
	fmt.Printf("town len max: %d\n", s.lenMax)
	fmt.Printf("temp min: %d\n", s.tempMin)
	fmt.Printf("temp max: %d\n", s.tempMax)

	close(done)
}

func main() {
	start := time.Now()
	defer func() {
		fmt.Printf("time main: %v\n", time.Since(start))
	}()

	if debug > 0 {
		fmt.Printf("debug limit: %v\n", debug)
	}

	pageCount = 1
	blockSize = int64(os.Getpagesize()) * pageCount

	fi, err := os.Stat(filename)
	oops(err)

	countBlocks := int(math.Round(float64(fi.Size()) / float64(blockSize)))

	blockCh = make(chan []byte, countBlocks)

	go readBlocks()
	go readMeasurements()

	wgReader := sync.WaitGroup{}

	for b := range blockCh {
		wgReader.Add(1)

		go func(b []byte) {
			defer wgReader.Done()

			scanBlock(b)
		}(b)
	}

	wgReader.Wait()

	close(measurements)

	<-done
}
