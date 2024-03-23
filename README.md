## 1brc

### Project description of 1brc

This is my GO implementation of the 1brc challenge of https://github.com/gunnarmorling/1brc

Running on my machine it takes about 7.776s to finish using SwissMap.

Using GO's original map implementation just add about 1 sec.

### Usage

You must generate the "measurements.txt" file like it is documented in  the 1brc challenge.

Copy or link the measurements.txt file into the directory of the main.go file or use the flag "-file" to define the path to "measurements.txt". 

To run the app and just print the calculated values

    go run . -file measurements.txt

To run the app and print the calculated values and counted lines and time needed

    go run . -file measurements.txt -verbose

To bench the app with GO tools

    go test -bench=. -file measurements.txt

### Results 

PC:

    OS: Pop!_OS 22.04 LTS x86_64
    Kernel: 6.6.10-76060610-generic
    Uptime: 2 days, 1 hour, 9 mins
    Packages: 2113 (dpkg), 6 (flatpak)
    Shell: bash 5.1.16
    Resolution: 2560x1440, 2880x2560
    DE: GNOME 42.5
    WM: Mutter
    WM Theme: Pop
    Theme: Pop [GTK2/3]
    Icons: Pop [GTK2/3]
    Terminal: tilix
    CPU: AMD Ryzen 7 3700X (16) @ 3.600GHz
    GPU: AMD ATI 0c:00.0 Device 744c
    Memory: 12362MiB / 64201MiB

    goos: linux
    goarch: amd64
    pkg: 1brc
    cpu: AMD Ryzen 7 3700X 8-Core Processor             
    BenchmarkMain-16    	       1	7063047022 ns/op
    PASS
    ok  	1brc	7.776s

Notebook:

	OS: Pop!_OS 22.04 LTS x86_64 
	Host: 83AR IdeaPad Pro 5 16APH8 
	Kernel: 6.6.10-76060610-generic 
	Uptime: 17 mins 
	Packages: 1972 (dpkg) 
	Shell: bash 5.1.16 
	Resolution: 2560x1600 
	DE: GNOME 42.5 
	WM: Mutter 
	WM Theme: Pop 
	Theme: Pop [GTK2/3] 
	Icons: Pop [GTK2/3] 
	Terminal: tilix 
	CPU: AMD Ryzen 7 7840HS (16) @ 5.137GHz 
	GPU: AMD ATI 64:00.0 Device 15bf 
	Memory: 3401MiB / 27841MiB 

    goos: linux
    goarch: amd64
    pkg: 1brc
    cpu: AMD Ryzen 7 7840HS with Radeon 780M Graphics
    BenchmarkMain-16    	       1	6587488481 ns/op
    PASS
    ok  	1brc	7.075s

### License

All software is copyright and protected by the Apache License, Version 2.0.
https://www.apache.org/licenses/LICENSE-2.0
