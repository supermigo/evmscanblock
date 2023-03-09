package daemon

import (
	"context"
	"os/signal"
	"syscall"
	"time"
)

// Daemon abstract a daemon.
type Daemon func()

// DaemonGenerator generates a Daemon
type DaemonGenerator func(ctx context.Context) (Daemon, error)

func SleepContext(ctx context.Context, delay time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

// BootstrapDaemons start daemons and handling os signal.
func BootstrapDaemons(ctx context.Context, daemonGenerators ...DaemonGenerator) {
	// os signal handling
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	// create daemon manager
	manager := newDaemonManager(ctx)
	manager.registerDaemons(daemonGenerators...)
	// wait os signal
	<-manager.ctx.Done()
	stop()
	// stop daemons
	manager.stop()
}

type daemonManager struct {
	ctx          context.Context
	daemonCtx    []context.Context
	daemonCancel []context.CancelFunc
	terminated   chan struct{}
}

func newDaemonManager(ctx context.Context) (d *daemonManager) {
	d = &daemonManager{
		ctx:        ctx,
		terminated: make(chan struct{}, 8),
	}
	return
}

func (d *daemonManager) registerDaemon(g DaemonGenerator) {
	if g != nil {
		ctx, cancel := context.WithCancel(context.Background())
		d.daemonCtx, d.daemonCancel = append(d.daemonCtx, ctx), append(d.daemonCancel, cancel)

		daemon, err := g(ctx)
		if err != nil {
			panic(err)
		}

		// start daemon
		go func() {
			daemon()
			d.terminated <- struct{}{}
		}()
	}
}

func (d *daemonManager) registerDaemons(gs ...DaemonGenerator) {
	for _, g := range gs {
		d.registerDaemon(g)
	}
}

func (d *daemonManager) stop() {
	for i := len(d.daemonCtx) - 1; i >= 0; i-- {
		d.daemonCancel[i]()
		<-d.terminated // wait termination
	}
	close(d.terminated)
}
