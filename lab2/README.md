### Raft Paper Notice

> 网络问题是最大的问题

期间遇到很多问题，最终发现都是因为自己没有理解 raft，并且在处理并发问题时没有考虑全面。



### 一个偶发性 bug

- 在 leader 同步日志的时候，如果因为网络问题而重发请求，但是在下一次重发请求之前，该 leader 变成了 follower，那么这时候不能重发请求，否则会错误。同步快照时同理
- 在相应日志同步/快照同步的时候，检查是否为 leader，如果是就返回 false
- 在接收日志同步/快照同步 reply 的时候，检查是否为 leader，如果不是就处理 reply



##### AppendEntries、CondInstallSnapshot 中一定要判断请求是否是“旧”的：因网络延迟、重发等原因，使得请求是曾经已经执行过的，或曾经已经执行过的请求包含该“旧”的请求！！！



以下是我未做完 lab2 时的记录，只记录了部分疑问点

---

> If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state. ($5.1)

如果 candidate 或者 leader 发现自己的 term 过期了，那么变成 follower，并用 RequestVote RPC 返回的 term 更新自己的 term。





##### election

- 从 follower 开始，增加 term，并变为 candidate。（$5.2）



##### log replication

>To bring a follower’s log into consistency with its own, the leader must find the latest log entry where the two logs agree, delete any entries in the follower’s log after that point, and send the follower all of the leader’s entries after that point.

- 当 leader 和 follower 日志不一致时，找到最新的两个日志一致的地方，删除 follower 中在该日志之后的日志，并将 leader 在该地方之后同步复制到 follower 中。($5.3)



##### follower

- 长时间没有收到来自 leader 的 heartbeats 或给 candidate 投票，则开始 election；否则保持 follower 状态。（$5.2）



##### candidate

- 向自己和集群中其他所有服务器发起投票（RequestVote RPC），直到下面任意一个发生：（$5.2）

  - 赢得选举。获得大多数选票，成为 leader。

  - 接收到其他服务器的 AppendEntries RPC。如果该 leader's term 小于该 candidate's term，则拒绝，该 leader 会转变为 follower。

  - 一段时间内仍没有 candidate 赢得选举。等待一定时间后发起新一轮 election。




##### leader

- 接收 client 的请求，将其保存在本地日志，将该请求的日志同步复制到其他服务器。leader 会定期无限重试同步日志（也可以设置超时，一定时间后停止重试），直到大多数服务器接收到了日志，则 leader 将该请求应用到本地，并将结果返回给 client。($5.2)







#### Q&A

Q：**A server remains in follower state as long as it receives valid RPCs from a leader or candidate.** Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries) to all followers in order to maintain their authority. If a follower receives no communication over a period of time called the election timeout, then it assumes there is no viable leader and begins an election to choose a new leader.  

原文意思是，follower 如果接收到 candidate 或者 leader 的指令，那么就会保持 follower 状态；也就是说，candidate 的投票指令（RequestVote RPC）也可以让 follower 保持状态。

但是我在网上看到的都是 follower 只能通过 leader 的心跳包来保持状态，并没有提到过 candidate 的投票指令。

**所以想问一下，follower 保持状态是只考虑 leader，还是也应该考虑 candidate ？**

A：If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.

leader 和 candidate 都应该考虑：leader 的 heartbeat 和 granting for voting to candidate 都会重置 follower 进入 election 阶段的 timeout 。



Q：当所有的服务器都是 candidate 时，candidate 之间又不能投票（election 给自己投票），那么如何结束 election 阶段呢？

A：服务器在任何状态下，如果某个 RPC 请求的参数或返回值中携带的 term 大于服务器自身的 currentTerm，那么服务器转变状态为 follower。又因为 election timeout 是一个随机值，所以不存在服务器一起变为 follower、一起进行 election。



Q：如果 leader 将一个新的日志同步复制到 follower 过程中崩溃，这时只有少数 follower 得到了这个日志，那么后面选举的时候，应该是复制了这个日志的 follower 成为 leader？还是其他未复制这个日志的 follower 成为 leader？还是都可以？

<img src="https://s2.loli.net/2022/08/15/RBs1tJizbOgqHpD.png" alt="image-20220815201131280" style="zoom:50%;" />

（s1 是 leader，同步日志到 s2 后崩溃）

A：都可以。客户端会重试，1）如果是 s2 当选 leader，s2 发现此次请求在 log 中已经存在，但是没有 commit，则增加一个冗余日志（新的 term），并同步、commit。2）如果 s3 当选 leader，s3 发现此次请求在 log 中不存在，则增加到 log 中，并同步、commit。



Q：在上面问题的情况下（client 向 s1 提交请求，s1 没有 commit 就宕机了），然后 client 也宕机了，并不重试，然后提交新的请求，结果会如何？

A：s2 和 s5 当选 leader 会导致不同的结果。如果持久化上一次提交的 id，在进行下次操作之前重试上次的操作，结果将唯一。









