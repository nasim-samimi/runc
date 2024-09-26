package libcontainer

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/opencontainers/runc/libcontainer/configs"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

func newStateTransitionError(from, to containerState) error {
	return &stateTransitionError{
		From: from.status().String(),
		To:   to.status().String(),
	}
}

// stateTransitionError is returned when an invalid state transition happens from one
// state to another.
type stateTransitionError struct {
	From string
	To   string
}

func (s *stateTransitionError) Error() string {
	return fmt.Sprintf("invalid state transition from %s to %s", s.From, s.To)
}

type containerState interface {
	transition(containerState) error
	destroy() error
	status() Status
}

func destroy(c *linuxContainer) error {
	if !c.config.Namespaces.Contains(configs.NEWPID) ||
		c.config.Namespaces.PathOf(configs.NEWPID) != "" {
		file, err := os.OpenFile("/home/worker3/debugdestroy.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		logger := log.New(file, "prefix", log.LstdFlags)
		logger.Printf("RemovePaths\n")
		paths := c.cgroupManager.GetPaths()

		filePath := filepath.Join(paths["cpu"], "cpu.rt_runtime_us")
		filePathmulti := filepath.Join(paths["cpu"], "cpu.rt_multi_runtime_us")

		removedRuntime, eread := readCpuRtRuntime(filePath)
		removedmultiRuntime, _ := readCpuRtRuntime(filePathmulti)
		logger.Printf("removedRuntime %v\n", removedRuntime)
		logger.Printf("removedmultiRuntime %v\n", removedmultiRuntime)
		logger.Printf("filepaths %v\n", filePath)
		if eread != nil {
			logger.Printf("error reading file %v\n", eread)
		}

		if err := signalAllProcesses(c.cgroupManager, unix.SIGKILL); err != nil {
			logrus.Warn(err)
		}
	}
	err := c.cgroupManager.Destroy()
	if c.intelRdtManager != nil {
		if ierr := c.intelRdtManager.Destroy(); err == nil {
			err = ierr
		}
	}
	if rerr := os.RemoveAll(c.root); err == nil {
		err = rerr
	}
	c.initProcess = nil
	if herr := runPoststopHooks(c); err == nil {
		err = herr
	}
	c.state = &stoppedState{c: c}
	return err
}
func readCpuRtRuntime(path string) (string, error) {

	buf, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	// runtimeStrings := strings.Split(string(buf), " ")
	// runtimeStrings = runtimeStrings[:len(runtimeStrings)-1]

	// runtime, err := strconv.ParseInt(string(buf), 10, 32)
	return string(buf), nil
}

func runPoststopHooks(c *linuxContainer) error {
	hooks := c.config.Hooks
	if hooks == nil {
		return nil
	}

	s, err := c.currentOCIState()
	if err != nil {
		return err
	}
	s.Status = specs.StateStopped

	if err := hooks[configs.Poststop].RunHooks(s); err != nil {
		return err
	}

	return nil
}

// stoppedState represents a container is a stopped/destroyed state.
type stoppedState struct {
	c *linuxContainer
}

func (b *stoppedState) status() Status {
	return Stopped
}

func (b *stoppedState) transition(s containerState) error {
	switch s.(type) {
	case *runningState, *restoredState:
		b.c.state = s
		return nil
	case *stoppedState:
		return nil
	}
	return newStateTransitionError(b, s)
}

func (b *stoppedState) destroy() error {
	return destroy(b.c)
}

// runningState represents a container that is currently running.
type runningState struct {
	c *linuxContainer
}

func (r *runningState) status() Status {
	return Running
}

func (r *runningState) transition(s containerState) error {
	switch s.(type) {
	case *stoppedState:
		if r.c.runType() == Running {
			return ErrRunning
		}
		r.c.state = s
		return nil
	case *pausedState:
		r.c.state = s
		return nil
	case *runningState:
		return nil
	}
	return newStateTransitionError(r, s)
}

func (r *runningState) destroy() error {
	if r.c.runType() == Running {
		return ErrRunning
	}
	return destroy(r.c)
}

type createdState struct {
	c *linuxContainer
}

func (i *createdState) status() Status {
	return Created
}

func (i *createdState) transition(s containerState) error {
	switch s.(type) {
	case *runningState, *pausedState, *stoppedState:
		i.c.state = s
		return nil
	case *createdState:
		return nil
	}
	return newStateTransitionError(i, s)
}

func (i *createdState) destroy() error {
	_ = i.c.initProcess.signal(unix.SIGKILL)
	return destroy(i.c)
}

// pausedState represents a container that is currently pause.  It cannot be destroyed in a
// paused state and must transition back to running first.
type pausedState struct {
	c *linuxContainer
}

func (p *pausedState) status() Status {
	return Paused
}

func (p *pausedState) transition(s containerState) error {
	switch s.(type) {
	case *runningState, *stoppedState:
		p.c.state = s
		return nil
	case *pausedState:
		return nil
	}
	return newStateTransitionError(p, s)
}

func (p *pausedState) destroy() error {
	t := p.c.runType()
	if t != Running && t != Created {
		if err := p.c.cgroupManager.Freeze(configs.Thawed); err != nil {
			return err
		}
		return destroy(p.c)
	}
	return ErrPaused
}

// restoredState is the same as the running state but also has associated checkpoint
// information that maybe need destroyed when the container is stopped and destroy is called.
type restoredState struct {
	imageDir string
	c        *linuxContainer
}

func (r *restoredState) status() Status {
	return Running
}

func (r *restoredState) transition(s containerState) error {
	switch s.(type) {
	case *stoppedState, *runningState:
		return nil
	}
	return newStateTransitionError(r, s)
}

func (r *restoredState) destroy() error {
	if _, err := os.Stat(filepath.Join(r.c.root, "checkpoint")); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	return destroy(r.c)
}

// loadedState is used whenever a container is restored, loaded, or setting additional
// processes inside and it should not be destroyed when it is exiting.
type loadedState struct {
	c *linuxContainer
	s Status
}

func (n *loadedState) status() Status {
	return n.s
}

func (n *loadedState) transition(s containerState) error {
	n.c.state = s
	return nil
}

func (n *loadedState) destroy() error {
	if err := n.c.refreshState(); err != nil {
		return err
	}
	return n.c.state.destroy()
}
