# Labs for mit-6.824
* [Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-1.html)
* [Lab: GFS](https://github.com/chaomai/goGFS)
    * 原有的实验框架使用的是net/rpc来实现rpc调用，但是由于client的调用都是短链接，server端主动关闭链接以后，会有大量的`TIME_WAIT`，这个是个优化点。
* [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
    * 由于在原有实验要求的基础上增加了一些优化，以及便于debug的目的，所以对test case做了修改。
    * 单次测试不一定能暴露出问题，可以使用如下方式，参数分别是：
        * 总共几轮
        * 每轮测试同时跑多少实例
        * 指定test case的名字
    
        ```bash
        bash -lx run.sh 20 4 TestFailNoAgree2B
        ```
