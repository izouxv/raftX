raftX
=====

基于Raft上写的一个类似Zookeeper的东东，业余爱好。半成品。
有ZK基本的功能。
用法可以看看raftX_test


$ raftX -p 4001 ~/node.1

$ raftX -p 4003 -join localhost:4001 ~/node.3
 