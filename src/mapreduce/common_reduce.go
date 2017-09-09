package mapreduce

// doReduce 负责一个 Reduce 任务：它会读入由 Map 阶段产生的中间结果键值对，
// 依键的顺序对键值对进行排序，调用用户指定的 Reduce 函数，并把输出写出到磁盘上
func doReduce(
	jobName string, // MapReduce 作业的名称
	reduceTaskNumber int, // 当前是哪个 Reduce 任务
	outFile string, // 输出文件路径
	nMap int, // Map 任务的数量
	reduceF func(key string, values []string) string,
) {
	// 你需要编写这个函数
	//
	// 你需要从每个 Map 任务读取一个中间文件，reduceName(jobName, m, reduceTaskNumber)
	// 会返回 Map 任务 m 中你需要读取的文件。
	//
	// 你的 doMap 函数会把键值对编码至中间文件中，因此你需要对它们进行解码。
	// 如果你使用了 JSON 格式，你可以读取文件的内容，再通过创建一个解码器
	// 并不断地对其调用 .Decode(&kv) 进行解码，直到它返回一个错误
	//
	// Go 语言 sort 包文档的第一个例子可能会对你有所帮助。
	//
	// reduceF() 是应用提供的 Reduce 函数。对于每一个不同的键你都需要调用它，
	// 并提供该键对应所有值组成的切片。reduceF() 会返回该键归约后的值。
	//
	// 你需要把 Reduce 输出的 KeyValue 以 JSON 格式写入到名为 outFile 的
	// 文件中。我们要求你使用 JSON 是因为后面负责对输出进行合并的 Merger
	// 期望这些输出是 JSON 格式的。JSON 格式本身没什么特别的，只是一种我们选
	// 择使用的编码格式罢了。你的输出代码看起来可能会像下面这样：
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}
