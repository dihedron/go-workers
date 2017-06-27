package workers

// Pool represents a pool of goroutines acting as workers.
type Pool struct {

	// count is the number of workers in the pool.
	count int

	// tasks is the channel onto which requests will be passed to
	// the underlying set of workers; each worker will wait on
	// the channel until it is closed (at pool shutdown).
	tasks chan interface{}

	// acknowledge is the channel used by workers to signal that
	// they are shutting down; this is necessary for the pool to
	// wait until all the enqueued tasks have been processed.
	acknowledge chan bool
}

// New creates a new Pool with the given number of workers.
func New(numWorkers int) *Pool {
	pool := &Pool{
		count:       numWorkers,
		tasks:       make(chan interface{}, 10000),
		acknowledge: make(chan bool, numWorkers),
	}
	return pool
}

// Start will start a pool of worker ready to apply the given
// TaskHandler to any incoming messages.
func (pool *Pool) Start(handler TaskHandler) {
	for i := 0; i < pool.count; i++ {
		go worker(i, pool.tasks, pool.acknowledge, nil, handler)
	}
}

// StartWithContext will start a pool of worker ready to apply the given
// TaskHandler to any incoming messages; the TaskContextGenerator will
// be invoked once for each worker in order to retrieve the appropriate
// context object.
func (pool *Pool) StartWithContext(handler TaskHandler, generator TaskContextGenerator) {
	for i := 0; i < pool.count; i++ {
		go worker(i, pool.tasks, pool.acknowledge, generator(i), handler)
	}
}

// Submit sends a task to the pool.
func (pool *Pool) Submit(task interface{}) {
	pool.tasks <- task
}

// WaitForCompletion waits until all the tasks have been
// processed, or all the workers have bailed out. In order to
// signal the workers that
func (pool *Pool) WaitForCompletion() {
	// close the tasks queue and wait for workers to
	// acknowledge or bail out forcibly
	close(pool.tasks)
	// Finally we collect all the results of the work.
	for i := 0; i < pool.count; i++ {
		<-pool.acknowledge
	}
}

// TaskHandler is a type of function called by the worker implementation
// to actually process an incoming request; the context can be nil, or
// anything produced by a ContextGenerator function at startup; the task is
// the message retrieved from the input channel. This function must return
// true to continue processing, or false to shut down this worker.
type TaskHandler func(context interface{}, task interface{}) bool

// TaskContextGenerator generates and initialises a context for each worker;
// it is provided an id for the worker.
type TaskContextGenerator func(id int) interface{}

// worker is the actual goroutine implementation, of which we'll run several
// concurrent instances. These workers will receive work on the `tasks` channel;
// once the input channel has been closed, they will return a boolean on the
// `acknowledge` channel.
func worker(id int, tasks <-chan interface{}, acknowledge chan<- bool, context interface{}, handler TaskHandler) {
	success := true
	for {
		task, ok := <-tasks
		if !ok {
			// let's aknowledge to the pool that we've sensed the
			// input channel has been closed
			acknowledge <- success
			break
		}

		success = success && handler(context, task)
		if !success {
			// the handler wants this worker to bail out
			acknowledge <- success
			break
		}
	}
}
