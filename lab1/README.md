# [MapReduce](http://nil.lcs.mit.edu/6.824/2020/labs/lab-mr.html)

![image-20220801101250074](https://s2.loli.net/2022/08/01/SkC1BceMjr4ypDV.png)

<center style="align: center; color: #888; font-size: 12px;">MapReduce Paper</center>

##### 工作整体流程

1. 一个 master 和多个 worker 并行工作，master 负责调度，worker 负责执行。
2. master 将输入任务分成 M 个任务（这个 lab 是多少个输入文件就是多少个任务），worker 获取其中一个任务，执行完毕后通知 master 该任务完成。
3. 待所有 map 任务完成后，master 将 map 阶段的结果转成 reduce 阶段的输入，然后交给 worker 执行，执行完毕后告诉 master 结果。

##### 一些细节

细节 lab 已经交代很清楚了（rules、hints），这里说一下自己做的时候一些问题



> 问题：reduce 阶段不同的输入文件可能含有相同的 key

刚开始想着是，map 阶段每个 worker 产生的中间键值对存成一个文件，比如：mr-1、mr-2、mr-3。但是这样会导致不同的文件中含有相同 key，进入 reduce 阶段前必须进行合并。如果进行合并的话，合并程序会成为性能瓶颈，在分布式中显然不合理。



> 解决：map 阶段将产生的键值对分区

如论文中的图画的一样，map 阶段每个任务不能只产生一个文件，产生 nReduce（reduce 并行数）个文件，如：mr-X-Y（X 为任务序号，Y 为键值对 hash(key) % nReduce），这样最终产生 M * R 个文件。master 将 Y 相等的文件放在一起，作为 reduce 阶段的任务。



当然中间也遇到了很多奇怪的 bug（rpc connection refused 等），不过都是一些编码上的错误。



##### lab

拷贝下课程代码，替换 mr 包下文件即可



##### 不足之处

- 因为还没有完整地阅读 mapreduce paper，不知道实现上哪里还有错误的地方（虽然 pass all tests）
- 因为文件内容会一次性全部读入，文件过大就会爆内存
- 还有一些其他分布式中的问题（fault tolerance 等）