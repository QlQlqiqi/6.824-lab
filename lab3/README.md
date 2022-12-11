一些问题：

1. 刚开始一直想这样一个场景：clerk 同时（一先一后）提交两个 append 操作（先 A 后 B），有两种情况：

   1. 当 A、B 都 committed 后、server 响应 clerk 之前；
   2. 当 A、B 都没有 committed 后、server 响应 clerk 之前；

   这个 server crash 了，然后 clerk retry，在新的 retry 请求到达新的 leader 之前，旧的 A、B 请求还没有 commit，这时候有两种情况：

   1. 新的 server 接收到的操作顺序是“先 B 后 A”；
   2. 新的 server 接收到的操作顺序是“先 A 后 B”；

   上面交叉可能会出现 4 种情况（甚至更多），都不容易处理。

   

   刚开始想这个想了很久，不知道如何处理，后面发现，如果规定：**clerk 在前面的操作没有结束之前，不能开始新的操作**，这样一来就简单多了。如果要一次请求多个，就用多个 clerk 就好。

2. 我这里把 get 和 update 操作视为一类。

3. 有一个 bug 改了我好几个 h，因为之前认为同一客户端连续发出的 get 都是一个 id，想着这样会快一点（事实上并没有快），后面几个 get 就不用占用时间了。事实上，如果客户端刚启动就请求 get，那么会因为起初的 id 为 0（因为 map[int] 的默认值为 0） 而导致 bug。可以让 id 不会成为 0 或者让每个 get 的 id 不一样。



流程：

1. clerk 发送请求到 client。
2. client 是否知道当前 leader 是谁：
   1. （不知道）client 认为每个 server 都是 leader，全部进行请求；
   2. （知道）client 对该 leader 进行请求；
3. client 收到来自 server 的响应有多种：
   1. 请求成功；
   2. 请求拒绝，因为自身不是 leader，并告知