## 1brc

### Project description of 1brc

This is my GO implementation of the 1brc challenge of https://github.com/gunnarmorling/1brc

Running on my machine it takes about 12.27 secs to finish using SwissMap.
Using GO's original map implementation add about 1 sec.


    goos: linux
    goarch: amd64
    pkg: 1brc
    cpu: AMD Ryzen 7 3700X 8-Core Processor             
    BenchmarkMain-16    	       1	12265206039 ns/op
    PASS
    ok  	1brc	12.276s

Here some infos of machine.

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
    

### License

All software is copyright and protected by the Apache License, Version 2.0.
https://www.apache.org/licenses/LICENSE-2.0