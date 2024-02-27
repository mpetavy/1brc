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

type dim struct {
	start int64
	end   int64
}

var filename = "/home/ransom/java/1brc/measurements.txt"
var blocksize = int64(1 * 256 * 1024)
var buf []byte
var dims chan dim

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
		var temp int64

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
				temp = temp * -1
			case '\n':
				break loop
			default:
				temp = temp * 10
				temp += int64(buf[i] - '0')
			}

			i++
		}

		town = town

		//fmt.Printf("%s:%d\n", town, temp)
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
		bufEnd := bufStart + blocksize
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

		dims <- dim{
			start: blockStart,
			end:   blockEnd,
		}

		blockStart = blockEnd + 1

		bufStart += int64(n)
	}

	close(dims)

	fmt.Printf("read:  %d\n", bufStart)
}

func main() {
	start := time.Now()
	defer func() {
		fmt.Printf("%v\n", time.Since(start))
	}()

	fi, err := os.Stat(filename)
	oops(err)

	count := int(math.Round(float64(fi.Size()) / float64(blocksize)))

	buf = make([]byte, fi.Size())
	dims = make(chan dim, count)

	go reader()

	wg := sync.WaitGroup{}

	mg := 0

	for dim := range dims {
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

	fmt.Printf("%d\n", mg)
}
