package main

import (
	"fmt"
	"mapreduce"
	"os"
)

//
// 对于每个输入文件，map 函数都会被调用一次。第一个参数是输入文件的名称，
// 而第二个参数则是文件的完成内容。你应当忽略输入文件的名称，只去关心文件
// 内容参数。返回值应是一个由键值对组成的切片
//
func mapF(filename string, contents string) []mapreduce.KeyValue {
	// TODO: 你需要编写这个函数
}

//
// 对于每个由 Map 任务产生的键，reduce 函数都会被调用一次，调用时传入
// 由所有 map 任务产生的与该键相关联的值的集合
//
func reduceF(key string, values []string) string {
	// TODO: 你需要编写这个函数
}

// 可以以 3 种方式执行
// 1) 顺序（如 go run wc.go master sequential x1.txt .. xN.txt）
// 2) Master（如 go run wc.go master localhost:7777 x1.txt .. xN.txt）
// 3) Worker（如 go run wc.go worker localhost:7777 localhost:7778 &）
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
