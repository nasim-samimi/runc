package fs

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/opencontainers/runc/libcontainer/cgroups"
	"github.com/opencontainers/runc/libcontainer/cgroups/fscommon"
	"github.com/opencontainers/runc/libcontainer/configs"
	"golang.org/x/sys/unix"
)

type CpuGroup struct{}

func (s *CpuGroup) Name() string {
	return "cpu"
}

func (s *CpuGroup) Apply(path string, r *configs.Resources, pid int) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	// We should set the real-Time group scheduling settings before moving
	// in the process because if the process is already in SCHED_RR mode
	// and no RT bandwidth is set, adding it will fail.
	if err := s.SetRtSched(path, r); err != nil {
		return err
	}
	// Since we are not using apply(), we need to place the pid
	// into the procs file.
	return cgroups.WriteCgroupProc(path, pid)
}

func (s *CpuGroup) SetRtSched(path string, r *configs.Resources) error {
	var period string
	// file, err := os.OpenFile("/home/worker3/debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer file.Close()
	// logger := log.New(file, "prefix", log.LstdFlags)
	if r.CpuRtPeriod != 0 {
		period = strconv.FormatUint(r.CpuRtPeriod, 10)
		if err := cgroups.WriteFile(path, "cpu.rt_period_us", period); err != nil {
			// The values of cpu.rt_period_us and cpu.rt_runtime_us
			// are inter-dependent and need to be set in a proper order.
			// If the kernel rejects the new period value with EINVAL
			// and the new runtime value is also being set, let's
			// ignore the error for now and retry later.
			if !errors.Is(err, unix.EINVAL) || r.CpuRtRuntime == 0 {
				return err
			}
		} else {
			period = ""
		}
		fmt.Println("cpu.rt_period_us", period)
	}

	if r.CpuRtRuntime != 0 {

		// Update the KubePods cgroup
		writeToParentMultiRuntime(filepath.Dir(filepath.Dir(filepath.Dir(path))), r)

		// Update the KubePodsBestEffort cgroup
		// cgroupKubePodsBestEffort := filepath.Join(cgroupBasePath, "kubepods.slice", "kubepods-besteffort.slice")
		writeToParentMultiRuntime(filepath.Dir(filepath.Dir(path)), r)

		// Update the pod cgroup
		writeToParentMultiRuntime(filepath.Dir(path), r)

		//write to container cgroup files
		containerRuntimeStr := r.CpusetCpus + " " + strconv.FormatInt(r.CpuRtRuntime, 10) + " "
		// logger.Printf("value of cpu.rt_multi_runtime_us %v\n in path:%v\n", containerRuntimeStr, path)
		if rerr := cgroups.WriteFile(path, "cpu.rt_multi_runtime_us", containerRuntimeStr); rerr != nil {
			return rerr
		}

		// logging data to debug.log

	}
	return nil
}

func readCpuRtMultiRuntimeFile(path string) ([]int64, error) {
	const (
		CpuRtMultiRuntimeFile = "cpu.rt_multi_runtime_us"
	)

	filePath := filepath.Join(path, CpuRtMultiRuntimeFile)
	buf, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	runtimeStrings := strings.Split(string(buf), " ")
	runtimeStrings = runtimeStrings[:len(runtimeStrings)-1]

	runtimes := make([]int64, 0, len(runtimeStrings))
	for _, runtimeStr := range runtimeStrings {
		v, err := strconv.ParseInt(runtimeStr, 10, 32)
		if err != nil {
			panic(fmt.Errorf("error parsing runtime %s in file %s: %v", runtimeStr, filePath, err))
		}
		runtimes = append(runtimes, v)
	}
	return runtimes, nil
}

func readCpuRtRuntimeFile(path string) (int64, error) {
	const (
		CpuRtMultiRuntimeFile = "cpu.rt_runtime_us"
	)

	filePath := filepath.Join(path, CpuRtMultiRuntimeFile)
	buf, err := os.ReadFile(filePath)
	if err != nil {
		return 0, err
	}

	runtimeStrings := strings.Split(string(buf), " ")
	runtimeStrings = runtimeStrings[:len(runtimeStrings)-1]

	runtime, err := strconv.ParseInt(runtimeStrings[0], 10, 32)
	return runtime, nil
}

func writeToParentMultiRuntime(path string, r *configs.Resources) error {
	const (
		parentRtPeriod = int64(1000000)
	)
	str := ""

	runtimes, _ := readCpuRtMultiRuntimeFile(path)

	containerCpuset := strings.Split(r.CpusetCpus, ",")
	addedRuntime := float64(0)

	addedRuntime = float64(r.CpuRtRuntime*parentRtPeriod/int64(r.CpuRtPeriod)) * float64(len(containerCpuset))

	newRuntime := int64(addedRuntime/float64(len(runtimes))) + runtimes[0]
	// averageRuntime := int64(addedRuntime/float64(len(containerCpuset))) + runtimes[0]
	// cpusetStr = "0-" + strconv.Itoa(len(runtimes)-1)
	// str = cpusetStr + " " + strconv.FormatInt(averageRuntime, 10)

	str = strconv.FormatInt(newRuntime, 10)
	if rerr := cgroups.WriteFile(path, "cpu.rt_runtime_us", str); rerr != nil {
		return rerr
	}

	return nil
}

func (s *CpuGroup) Set(path string, r *configs.Resources) error {
	if r.CpuShares != 0 {
		shares := r.CpuShares
		if err := cgroups.WriteFile(path, "cpu.shares", strconv.FormatUint(shares, 10)); err != nil {
			return err
		}
		// read it back
		sharesRead, err := fscommon.GetCgroupParamUint(path, "cpu.shares")
		if err != nil {
			return err
		}
		// ... and check
		if shares > sharesRead {
			return fmt.Errorf("the maximum allowed cpu-shares is %d", sharesRead)
		} else if shares < sharesRead {
			return fmt.Errorf("the minimum allowed cpu-shares is %d", sharesRead)
		}
	}

	var period string
	if r.CpuPeriod != 0 {
		period = strconv.FormatUint(r.CpuPeriod, 10)
		if err := cgroups.WriteFile(path, "cpu.cfs_period_us", period); err != nil {
			// Sometimes when the period to be set is smaller
			// than the current one, it is rejected by the kernel
			// (EINVAL) as old_quota/new_period exceeds the parent
			// cgroup quota limit. If this happens and the quota is
			// going to be set, ignore the error for now and retry
			// after setting the quota.
			if !errors.Is(err, unix.EINVAL) || r.CpuQuota == 0 {
				return err
			}
		} else {
			period = ""
		}
	}
	if r.CpuQuota != 0 {
		if err := cgroups.WriteFile(path, "cpu.cfs_quota_us", strconv.FormatInt(r.CpuQuota, 10)); err != nil {
			return err
		}
		if period != "" {
			if err := cgroups.WriteFile(path, "cpu.cfs_period_us", period); err != nil {
				return err
			}
		}
	}
	return s.SetRtSched(path, r)
}

func (s *CpuGroup) GetStats(path string, stats *cgroups.Stats) error {
	const file = "cpu.stat"
	f, err := cgroups.OpenFile(path, file, os.O_RDONLY)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		t, v, err := fscommon.ParseKeyValue(sc.Text())
		if err != nil {
			return &parseError{Path: path, File: file, Err: err}
		}
		switch t {
		case "nr_periods":
			stats.CpuStats.ThrottlingData.Periods = v

		case "nr_throttled":
			stats.CpuStats.ThrottlingData.ThrottledPeriods = v

		case "throttled_time":
			stats.CpuStats.ThrottlingData.ThrottledTime = v
		}
	}
	return nil
}
