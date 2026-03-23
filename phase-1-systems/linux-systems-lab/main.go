package main

import (
"crypto/sha256"
"encoding/hex"
"encoding/json"
"fmt"
"io"
"log"
"math/rand"
"net/http"
"os"
"runtime"
"strconv"
"strings"
"sync"
"time"
)

type response struct {
	OK      bool   `json:"ok"`
	Detail  string `json:"detail"`
	Elapsed string `json:"elapsed"`
}

func writeJSON(w http.ResponseWriter, status int, msg response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(msg)
}

func cpuHandler(w http.ResponseWriter, r *http.Request) {
	loops := parseInt(r, "loops", 3000000)
	start := time.Now()

	h := sha256.New()
	for i := 0; i < loops; i++ {
		io.WriteString(h, strconv.Itoa(i))
	}
	sum := hex.EncodeToString(h.Sum(nil))

	writeJSON(w, http.StatusOK, response{
OK:      true,
Detail:  fmt.Sprintf("cpu loop done, hash-prefix=%s", sum[:12]),
Elapsed: time.Since(start).String(),
	})
}

func ioHandler(w http.ResponseWriter, r *http.Request) {
	mb := parseInt(r, "mb", 32)
	start := time.Now()

	f, err := os.CreateTemp("", "lab-io-*.dat")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, response{OK: false, Detail: err.Error()})
		return
	}
	path := f.Name()
	defer os.Remove(path)
	defer f.Close()

	buf := make([]byte, 1024*1024)
	_, _ = rand.Read(buf)
	for i := 0; i < mb; i++ {
		if _, err := f.Write(buf); err != nil {
			writeJSON(w, http.StatusInternalServerError, response{OK: false, Detail: err.Error()})
			return
		}
	}
	if err := f.Sync(); err != nil {
		writeJSON(w, http.StatusInternalServerError, response{OK: false, Detail: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, response{
OK:      true,
Detail:  fmt.Sprintf("wrote %dMB to %s", mb, path),
Elapsed: time.Since(start).String(),
	})
}

func memHandler(w http.ResponseWriter, r *http.Request) {
	mb := parseInt(r, "mb", 128)
	start := time.Now()

	chunks := make([][]byte, 0, mb)
	for i := 0; i < mb; i++ {
		b := make([]byte, 1024*1024)
		b[0] = byte(i % 255)
		chunks = append(chunks, b)
	}
	runtime.KeepAlive(chunks)

	writeJSON(w, http.StatusOK, response{
OK:      true,
Detail:  fmt.Sprintf("allocated ~%dMB", mb),
Elapsed: time.Since(start).String(),
	})
}

func mixedHandler(w http.ResponseWriter, r *http.Request) {
	workers := parseInt(r, "workers", 4)
	loops := parseInt(r, "loops", 1000000)
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			h := sha256.New()
			for j := 0; j < loops; j++ {
				s := strings.Builder{}
				s.WriteString(strconv.Itoa(seed))
				s.WriteString(":")
				s.WriteString(strconv.Itoa(j))
				io.WriteString(h, s.String())
			}
		}(i)
	}
	wg.Wait()

	writeJSON(w, http.StatusOK, response{
OK:      true,
Detail:  fmt.Sprintf("mixed workload with %d workers", workers),
Elapsed: time.Since(start).String(),
	})
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, response{OK: true, Detail: "healthy", Elapsed: "0s"})
}

func parseInt(r *http.Request, key string, fallback int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/cpu", cpuHandler)
	mux.HandleFunc("/io", ioHandler)
	mux.HandleFunc("/mem", memHandler)
	mux.HandleFunc("/mixed", mixedHandler)

	addr := ":8080"
	log.Printf("linux systems lab service listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
