# Labs for mit-6.824

* [Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-1.html)
* [Lab: GFS](https://github.com/chaomai/mit-6.824/tree/master/ppca-gfs)
    1. 原有的实验框架使用的是net/rpc来实现rpc调用，但是由于client的调用都是短链接，server端主动关闭链接以后，会有大量的`TIME_WAIT`，这个是个优化点。
* [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
