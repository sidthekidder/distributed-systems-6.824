package mapreduce

import (
	"fmt"
	"sync"
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
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	var wg sync.WaitGroup

	// loop through all the ntasks to be done, waiting for workers using the registerChan channel
	for tasksIndex := 0 ; tasksIndex < ntasks ; tasksIndex++ {
		// increment the WaitGroup
		wg.Add(1)

		// launch the goroutine and later call Done for each finished goroutine
		go func(jobName string, mapFiles []string, phase jobPhase, tasksIndex int, n_other int, registerChan chan string) {
			for {
				rpc_info_str := <- registerChan

				ok := call(rpc_info_str, "Worker.DoTask", DoTaskArgs{jobName, mapFiles[tasksIndex], phase, tasksIndex, n_other}, nil)
				if ok == true {
					// call waitgroup.Done() before sending working back to registerChan
					// because it blocks till it is received and for the 20th element there are no more receivers
					wg.Done()
					registerChan <- rpc_info_str
					break
				}
			}
		}(jobName, mapFiles, phase, tasksIndex, n_other, registerChan)
	}

	// wait for all the goroutines to finish
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
	return
}
