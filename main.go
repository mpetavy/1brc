package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"runtime"
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

var filename = "/home/ransom/java/1brc/measurements.txt"
var buf []byte
var blockSize = int64(1 * 256 * 1024)
var blockCh chan block

var mu = sync.Mutex{}
var lenMin = 100
var lenMax = 0
var tempMin = 1000
var tempMax = 0

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

func scan(start int64, end int64) {
	offset := int64(2)

	for i := start; i <= end; i++ {
		var town string
		var temp int
		var minus bool

		lastI := i
		i += offset

		for {
			if buf[i] == ';' {
				town = string(buf[lastI:i])
				i += 1

				break
			}

			i++
		}

	loop:
		for {
			switch buf[i] {
			case '.':
			case '-':
				minus = true
			case '\n':
				break loop
			default:
				temp = temp * 10
				temp += int(buf[i] - '0')
			}

			i++
		}

		if minus {
			temp = temp * -1
		}

		mu.Lock()

		l := len(town)
		lenMin = min(lenMin, l)
		lenMax = max(lenMax, l)

		tempMin = min(tempMin, temp)
		tempMax = max(tempMax, temp)

		mu.Unlock()
	}
}

func reader() {
	file, err := os.Open(filename)
	oops(err)

	defer func() {
		oops(file.Close())
	}()

	var bufStart int64
	var blockStart int64
	var blockEnd int64
	buflen := int64(len(buf))

loop:
	for {
		bufEnd := bufStart + blockSize
		if bufEnd > buflen {
			bufEnd = buflen
		}

		if bufStart == bufEnd {
			break loop
		}

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

	fmt.Printf("read:  %d\n", bufStart)
}

func main() {
	start := time.Now()
	defer func() {
		fmt.Printf("%v\n", time.Since(start))
	}()

	fi, err := os.Stat(filename)
	oops(err)

	count := int(math.Round(float64(fi.Size()) / float64(blockSize)))

	buf = make([]byte, fi.Size())
	blockCh = make(chan block, count)

	go reader()

	wg := sync.WaitGroup{}

	mg := 0

	for dim := range blockCh {
		wg.Add(1)
		gr := runtime.NumGoroutine()
		if mg < gr {
			mg = gr
		}
		go func(start int64, end int64) {
			defer wg.Done()

			scan(start, end)
		}(dim.start, dim.end)
	}

	wg.Wait()

	fmt.Printf("max go routines used: %d\n", mg)
	fmt.Printf("town len min: %d\n", lenMin)
	fmt.Printf("town len max: %d\n", lenMax)
	fmt.Printf("temp min: %d\n", tempMin)
	fmt.Printf("temp max: %d\n", tempMax)
}
