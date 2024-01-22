package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type block []byte

type values struct {
	count int
	min   int
	max   int
}

var (
	block_count = runtime.NumCPU()
	block_size  = 1024 * 256
	//file_name   = "/home/ransom/Downloads/VeraCrypt_1.26.7.tar.gz"
	//file_name   = "/home/ransom/java/1brc/README.md"
	file_name = "/home/ransom/java/1brc/measurements.txt"

	workBlocks chan block
	freeBlocks chan block
	done       = make(chan struct{})
	keyvalues  = make(map[string]float64)
	mu         sync.Mutex
)

func oops(err error) {
	if err == nil {
		return
	}

	panic(err)
}

func PrintBytes(ba []byte, breakOnLineEndings bool) {
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

func filereader() {
	file, err := os.Open(file_name)
	oops(err)

	defer func() {
		oops(file.Close())
	}()

	var w int64 = 0

loop:
	for b := range freeBlocks {
		n, err := file.Read(b)

		w += int64(n)

		switch err {
		case io.EOF:
			close(workBlocks)

			break loop
		case nil:
			if n < len(b) {
				workBlocks <- b[:n]
			} else {
				workBlocks <- b
			}
		default:
			oops(err)
		}
	}

	fmt.Printf("read:  %d\n", w)
}

func copier() {
	file, err := os.Create("./out")
	oops(err)

	defer func() {
		oops(file.Close())
	}()

	var w int64 = 0

	for b := range workBlocks {
		n, err := file.Write(b)
		oops(err)

		w += int64(n)

		if n != len(b) {
			oops(fmt.Errorf("len mismatch"))
		}

		freeBlocks <- b
	}

	fmt.Printf("write: %d\n", w)

	close(done)
}

func add(k []byte, v []byte) {
	return

	key := string(k)

	value, err := strconv.ParseFloat(string(v), 64)
	oops(err)

	mu.Lock()
	keyvalues[key] = value
	mu.Unlock()
}

func worker() {
	var p []byte
	var s, c int

	for b := range workBlocks {
		b = append(p, b...)
		s = 0

		for i := 0; i < len(b); i++ {
			ch := b[i]
			switch ch {
			case ';':
				c = i
			case '\n':
				go add(b[s:c], b[c+1:i])
				//key := string(b[s:c])
				//
				//value, err := strconv.ParseFloat(string(b[c+1:i]), 64)
				//oops(err)
				//
				//keyvalues[key] = value
				//
				//key = key
				//value = value

				//fmt.Printf("%s: %f\n", key, value)

				s = i + 1
			}
		}

		if s < len(b) {
			clear(p)
			p = append(p, b[s:]...)
			c = s - c
		}

		freeBlocks <- b
	}

	close(done)
}

func main() {
	start := time.Now()
	defer func() {
		fmt.Printf("%v\n", time.Since(start))
	}()

	workBlocks = make(chan block, block_count)
	freeBlocks = make(chan block, block_count)

	for i := 0; i < block_count; i++ {
		freeBlocks <- make([]byte, block_size)
	}

	go filereader()
	//go copier()
	go worker()

	var keys []string
	for k := range keyvalues {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		fmt.Printf("%s: %f", k, keyvalues[k])
	}

	<-done
}
