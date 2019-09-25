# Labs for mit-6.824
* [Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-1.html)
* [Lab: GFS](https://github.com/chaomai/goGFS)
    * 原有的实验框架使用的是net/rpc来实现rpc调用，但是由于client的调用都是短链接，server端主动关闭链接以后，会有大量的`TIME_WAIT`，这个是个优化点。
* [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
    * 单次测试不一定能暴露出问题，可以使用如下方式，
        ```bash
        for ((i=1;i<=20;i++)); do
            max_j=9
            for ((j=1;j<=$max_j;j++)); do
                go test -race -v -run TestInitialElection2A > TestRet${i}${j} &
            done
            go test -race -v -run TestInitialElection2A > TestRet${i}${j}
        done
        ```
