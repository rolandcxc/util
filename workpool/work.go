package workpool

import (
	"context"
	"log"
)

type Job interface {
	run(ctx context.Context)
}

func worker(ctx context.Context, id int, jobs <-chan Job, ends chan<- Job) {
	log.Printf("worker %d start", id)
	for job := range jobs {
		func() {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("job panic: %s", err)
				}
				ends <- job
			}()

			job.run(ctx)
		}()
	}
	log.Printf("worker %d quit", id)
}

func RunJobs(ctx context.Context, workerNum int, jobs []Job) {
	numJobs := len(jobs)

	jobChan := make(chan Job, numJobs)
	ends := make(chan Job, numJobs)

	for w := 1; w <= workerNum; w++ {
		go worker(ctx, w, jobChan, ends)
	}

	for _, job := range jobs {
		jobChan <- job
	}
	close(jobChan)

	for a := 1; a <= numJobs; a++ {
		<-ends
	}
}

type FuncJob func(ctx context.Context)

func funcWorker(ctx context.Context, id int, funcJobs <-chan FuncJob, ends chan<- struct{}) {
	log.Printf("worker %d start", id)
	for f := range funcJobs {
		func() {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("job panic: %s", err)
				}
				ends <- struct{}{}
			}()
			f(ctx)
		}()
	}
	log.Printf("worker %d quit", id)
}

func RunFuncJobsWithWorkerNum(ctx context.Context, jobs []FuncJob, workerNum int) {
	numJobs := len(jobs)

	jobChan := make(chan FuncJob, numJobs)
	ends := make(chan struct{}, numJobs)

	for w := 1; w <= workerNum; w++ {
		go funcWorker(ctx, w, jobChan, ends)
	}

	for _, job := range jobs {
		jobChan <- job
	}
	close(jobChan)

	for a := 1; a <= numJobs; a++ {
		<-ends
	}
}

func RunFuncJobs(ctx context.Context, jobs []FuncJob) {
	numJobs := len(jobs)
	RunFuncJobsWithWorkerNum(ctx, jobs, getDefaultWorkerNum(numJobs))
}

func getDefaultWorkerNum(jobNum int) int {
	const MaxWorkerNum = 10
	if jobNum < MaxWorkerNum {
		return jobNum
	}
	return MaxWorkerNum
}
