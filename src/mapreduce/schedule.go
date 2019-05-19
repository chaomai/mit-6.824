package mapreduce

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	idleChan := make(chan string)

	go func() {
		fmt.Println("listen to registerChan")
		for {
			w := <-registerChan

			fmt.Printf("get new worker[%s]\n", w)
			idleChan <- w
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(ntasks)

	taskChan := make(chan *DoTaskArgs, ntasks)

	for i := 0; i < ntasks; i++ {
		doTaskArgs := new(DoTaskArgs)
		doTaskArgs.JobName = jobName
		doTaskArgs.Phase = phase
		doTaskArgs.TaskNumber = i
		doTaskArgs.NumOtherPhase = nOther

		switch phase {
		case mapPhase:
			doTaskArgs.File = mapFiles[i]
		case reducePhase:
		}

		taskChan <- doTaskArgs
	}

	var taskIdx int32

loop:
	for {
		select {
		case worker := <-idleChan:
			select {
			case task := <-taskChan:
				go func() {
					if ok := call(worker, "Worker.DoTask", task, nil); !ok {
						taskChan <- task
					} else {
						fmt.Printf("task[%d] for worker[%s] is done\n", task.TaskNumber, worker)
						wg.Done()
						idleChan <- worker

						atomic.AddInt32(&taskIdx, 1)
					}
				}()
			default:
				fmt.Println("no new tasks")
			}
		default:
			fmt.Println("wait for idle worker")
			time.Sleep(time.Millisecond)
		}

		if atomic.LoadInt32(&taskIdx) == int32(ntasks) {
			break loop
		}
	}

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
