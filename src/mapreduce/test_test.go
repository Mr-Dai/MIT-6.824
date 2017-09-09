package mapreduce

import (
	"fmt"
	"testing"
	"time"

	"bufio"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	nNumber = 100000
	nMap    = 100
	nReduce = 50
)

// 创建有 N 个数字的输入文件
// 检查输出文件中是否有 N 个数字

// 以单词分隔
func MapFunc(file string, value string) (res []KeyValue) {
	debug("Map %v\n", value)
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}

// 直接返回键
func ReduceFunc(key string, values []string) string {
	for _, e := range values {
		debug("Reduce %s %v\n", key, e)
	}
	return ""
}

// 将输入文件与输出文件比对：所有数字都应出现在输出文件中，以字典序排序
func check(t *testing.T, files []string) {
	output, err := os.Open("mrtmp.test")
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		if err != nil {
			log.Fatal("check: ", err)
		}
		defer input.Close()
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i++
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Worker 会在 Shutdown 的响应中返回它们处理了多少个 RPC 调用。
// 确认它们处理了至少一个 DoTask RPC 调用
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("A worker didn't do any work\n")
		}
	}
}

// 创建输入文件
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			log.Fatal("mkInput: ", err)
		}
		w := bufio.NewWriter(file)
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}

// 在 /var/tmp 中构建一个唯一的 Unix 域套接字名。
// 不能在当前目录下做，因为 AFS 不支持 Unix 域套接字
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *Master {
	files := makeInputs(nMap)
	master := port("master")
	mr := Distributed("test", files, nReduce, master)
	return mr
}

func cleanup(mr *Master) {
	mr.CleanupFiles()
	for _, f := range mr.files {
		removeFile(f)
	}
}

func TestSequentialSingle(t *testing.T) {
	mr := Sequential("test", makeInputs(1), 1, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestSequentialMany(t *testing.T) {
	mr := Sequential("test", makeInputs(5), 3, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestBasic(t *testing.T) {
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1)
	}
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestOneFailure(t *testing.T) {
	mr := setup()
	// 启动两个会在 10 个任务后失效的 Worker
	go RunWorker(mr.address, port("worker"+strconv.Itoa(0)),
		MapFunc, ReduceFunc, 10)
	go RunWorker(mr.address, port("worker"+strconv.Itoa(1)),
		MapFunc, ReduceFunc, -1)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.doneChannel:
			check(t, mr.files)
			cleanup(mr)
			break
		default:
			// 每秒启动两个 Worker。这些 Worker 会在 10 个任务后失效
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}
