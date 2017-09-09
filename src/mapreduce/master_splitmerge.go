package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

// merge 会使用归并排序把若干个 Reduce 任务的输出结果合并为一个输出文件
func (mr *Master) merge() {
	debug("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := mergeName(mr.jobName, i)
		fmt.Printf("Merge: read %s\n", p)
		file, err := os.Open(p)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create("mrtmp." + mr.jobName)
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}

// removeFile 会调用 os.Remove，并在发生错误时用日志记录错误
func removeFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}

// CleanupFiles 会删除所有由正在运行的 mr 作业生成的中间文件
func (mr *Master) CleanupFiles() {
	for i := range mr.files {
		for j := 0; j < mr.nReduce; j++ {
			removeFile(reduceName(mr.jobName, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		removeFile(mergeName(mr.jobName, i))
	}
	removeFile("mrtmp." + mr.jobName)
}
