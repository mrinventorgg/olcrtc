// Package supervisor runs ordered session profiles with failover.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/app/session"
)

const DefaultRetryDelay = 2 * time.Second
const DefaultHistoryLimit = 20

const (
	// EventProfileStart marks a profile attempt starting.
	EventProfileStart = "profile_start"
	// EventProfileEnd marks a profile attempt ending.
	EventProfileEnd = "profile_end"
)

var (
	// ErrNoProfiles is returned when the supervisor is started without profiles.
	ErrNoProfiles = errors.New("supervisor: no profiles configured")
	// ErrMaxCyclesExceeded is returned after MaxCycles complete profile-list passes.
	ErrMaxCyclesExceeded = errors.New("supervisor: max failover cycles exceeded")
)

// Profile is one runnable session configuration in an ordered failover list.
type Profile struct {
	Name   string
	Config session.Config
}

// ProfileStatus summarizes one profile's failover history.
type ProfileStatus struct {
	Name        string
	Starts      int
	Failures    int
	CleanEnds   int
	LastStarted time.Time
	LastEnded   time.Time
	LastError   string
}

// Event is one bounded failover history entry.
type Event struct {
	Time    time.Time
	Type    string
	Profile string
	Cycle   int
	Error   string
}

// Status is a point-in-time view of the supervisor.
type Status struct {
	Cycle              int
	ActiveProfile      string
	ActiveProfileIndex int
	Profiles           []ProfileStatus
	History            []Event
	LastError          string
}

// Runner starts one session profile and blocks until it ends or fails.
type Runner func(ctx context.Context, cfg session.Config) error

// Config controls ordered failover behavior.
type Config struct {
	Profiles   []Profile
	RetryDelay time.Duration
	MaxCycles  int

	OnProfileStart func(profile Profile, cycle int)
	OnProfileEnd   func(profile Profile, cycle int, err error)
	OnStatus       func(status Status)
	HistoryLimit   int
}

// Run starts profiles in order. If a profile exits while ctx is still active,
// the supervisor waits RetryDelay and advances to the next profile.
func Run(ctx context.Context, cfg Config, run Runner) error {
	if len(cfg.Profiles) == 0 {
		return ErrNoProfiles
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = DefaultRetryDelay
	}
	state := newStatusTracker(cfg.Profiles, cfg.HistoryLimit, cfg.OnStatus)

	var lastErr error
	for cycle := 1; ; cycle++ {
		for i, profile := range cfg.Profiles {
			if ctx.Err() != nil {
				return nil
			}
			state.start(i, cycle)
			if cfg.OnProfileStart != nil {
				cfg.OnProfileStart(profile, cycle)
			}

			err := run(ctx, profile.Config)
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				lastErr = fmt.Errorf("profile %q: %w", profile.Name, err)
			} else {
				lastErr = fmt.Errorf("profile %q ended", profile.Name)
			}
			state.end(i, cycle, err)
			if cfg.OnProfileEnd != nil {
				cfg.OnProfileEnd(profile, cycle, err)
			}

			if cfg.MaxCycles > 0 && cycle >= cfg.MaxCycles && i == len(cfg.Profiles)-1 {
				return fmt.Errorf("%w after %d cycle(s): %w", ErrMaxCyclesExceeded, cycle, lastErr)
			}
			if err := waitRetryDelay(ctx, cfg.RetryDelay); err != nil {
				return nil
			}
		}
	}
}

type statusTracker struct {
	status       Status
	notify       func(Status)
	historyLimit int
}

func newStatusTracker(profiles []Profile, historyLimit int, notify func(Status)) *statusTracker {
	if historyLimit == 0 {
		historyLimit = DefaultHistoryLimit
	}
	statusProfiles := make([]ProfileStatus, 0, len(profiles))
	for _, profile := range profiles {
		statusProfiles = append(statusProfiles, ProfileStatus{Name: profile.Name})
	}
	return &statusTracker{
		status: Status{
			ActiveProfileIndex: -1,
			Profiles:           statusProfiles,
		},
		notify:       notify,
		historyLimit: historyLimit,
	}
}

func (t *statusTracker) start(profileIndex, cycle int) {
	now := time.Now()
	profile := &t.status.Profiles[profileIndex]
	profile.Starts++
	profile.LastStarted = now
	t.status.Cycle = cycle
	t.status.ActiveProfile = profile.Name
	t.status.ActiveProfileIndex = profileIndex
	t.appendHistory(Event{
		Time:    now,
		Type:    EventProfileStart,
		Profile: profile.Name,
		Cycle:   cycle,
	})
	t.emit()
}

func (t *statusTracker) end(profileIndex, cycle int, err error) {
	now := time.Now()
	profile := &t.status.Profiles[profileIndex]
	profile.LastEnded = now
	event := Event{
		Time:    now,
		Type:    EventProfileEnd,
		Profile: profile.Name,
		Cycle:   cycle,
	}
	if err != nil {
		profile.Failures++
		profile.LastError = err.Error()
		t.status.LastError = fmt.Sprintf("profile %q: %v", profile.Name, err)
		event.Error = err.Error()
	} else {
		profile.CleanEnds++
		profile.LastError = ""
		t.status.LastError = fmt.Sprintf("profile %q ended", profile.Name)
	}
	t.status.ActiveProfile = ""
	t.status.ActiveProfileIndex = -1
	t.appendHistory(event)
	t.emit()
}

func (t *statusTracker) appendHistory(event Event) {
	if t.historyLimit < 0 {
		return
	}
	t.status.History = append(t.status.History, event)
	if len(t.status.History) > t.historyLimit {
		t.status.History = t.status.History[len(t.status.History)-t.historyLimit:]
	}
}

func (t *statusTracker) emit() {
	if t.notify == nil {
		return
	}
	t.notify(cloneStatus(t.status))
}

func cloneStatus(status Status) Status {
	status.Profiles = append([]ProfileStatus(nil), status.Profiles...)
	status.History = append([]Event(nil), status.History...)
	return status
}

func waitRetryDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
