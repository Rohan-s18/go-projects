package mapreduce

import (
	"container/list"
	"fmt"
)

type WorkerInfo struct {
	address string
}

type jobResult struct {
	worker    string
	jobNumber int
	ok        bool
}

// runPhase executes either all map jobs or all reduce jobs.
func (mr *MapReduce) runPhase(
	operation JobType,
	numberOfJobs int,
	numberOfOtherPhaseJobs int,
	availableWorkers []string,
) []string {

	pendingJobs := make([]int, numberOfJobs)
	for job := 0; job < numberOfJobs; job++ {
		pendingJobs[job] = job
	}

	results := make(chan jobResult, numberOfJobs)
	completedJobs := 0

	for completedJobs < numberOfJobs {
		// Assign as many pending jobs as possible.
		for len(pendingJobs) > 0 && len(availableWorkers) > 0 {
			job := pendingJobs[0]
			pendingJobs = pendingJobs[1:]

			last := len(availableWorkers) - 1
			worker := availableWorkers[last]
			availableWorkers = availableWorkers[:last]

			args := DoJobArgs{
				File:          mr.file,
				Operation:     operation,
				JobNumber:     job,
				NumOtherPhase: numberOfOtherPhaseJobs,
			}

			go func(worker string, args DoJobArgs) {
				var reply DoJobReply

				ok := call(
					worker,
					"Worker.DoJob",
					&args,
					&reply,
				)

				results <- jobResult{
					worker:    worker,
					jobNumber: args.JobNumber,
					ok:        ok && reply.OK,
				}
			}(worker, args)
		}

		// If every pending job is currently running, wait for one result.
		// Avoid accepting idle workers here because the tests expect every
		// recorded worker to have processed at least one job.
		if len(pendingJobs) == 0 {
			result := <-results

			if result.ok {
				completedJobs++
				availableWorkers = append(
					availableWorkers,
					result.worker,
				)
			} else {
				// The worker failed, so retry its job on another worker.
				pendingJobs = append(
					pendingJobs,
					result.jobNumber,
				)
			}

			continue
		}

		// No worker is currently available. Wait for either a new worker
		// or a result from an existing worker.
		select {
		case worker := <-mr.registerChannel:
			if _, exists := mr.Workers[worker]; !exists {
				mr.Workers[worker] = &WorkerInfo{
					address: worker,
				}
			}

			availableWorkers = append(availableWorkers, worker)

		case result := <-results:
			if result.ok {
				completedJobs++
				availableWorkers = append(
					availableWorkers,
					result.worker,
				)
			} else {
				// Do not reuse the failed worker.
				pendingJobs = append(
					pendingJobs,
					result.jobNumber,
				)
			}
		}
	}

	return availableWorkers
}

// Clean up all workers and collect how many jobs each performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()

	for _, worker := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", worker.address)

		args := &ShutdownArgs{}
		var reply ShutdownReply

		ok := call(
			worker.address,
			"Worker.Shutdown",
			args,
			&reply,
		)

		if !ok {
			fmt.Printf(
				"DoWork: RPC %s shutdown error\n",
				worker.address,
			)
		} else {
			l.PushBack(reply.Njobs)
		}
	}

	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	availableWorkers := make([]string, 0)

	// Every map job generates one partition per reduce job.
	availableWorkers = mr.runPhase(
		Map,
		mr.nMap,
		mr.nReduce,
		availableWorkers,
	)

	// Every reduce job reads output from every map job.
	availableWorkers = mr.runPhase(
		Reduce,
		mr.nReduce,
		mr.nMap,
		availableWorkers,
	)

	return mr.KillWorkers()
}
