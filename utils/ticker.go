package utils

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// inspired by https://github.com/krayzpipes/cronticker/blob/main/cronticker/ticker.go

type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

type StdTicker struct {
	*time.Ticker
}

func (g *StdTicker) Chan() <-chan time.Time {
	return g.C
}

func NewStdTicker(d time.Duration) *StdTicker {
	return &StdTicker{time.NewTicker(d)}
}

var _ Ticker = (*StdTicker)(nil)

// CronTicker is the struct returned to the user as a proxy
// to the ticker. The user can check the ticker channel for the next
// 'tick' via CronTicker.C (similar to the user of time.Timer).
type CronTicker struct {
	C chan time.Time
	k chan bool
}

var _ Ticker = (*CronTicker)(nil)

// Stop sends the appropriate message on the control channel to
// kill the CronTicker goroutines. It's good practice to use `defer CronTicker.Stop()`.
func (c *CronTicker) Stop() {
	c.k <- true
}

func (c *CronTicker) Chan() <-chan time.Time {
	return c.C
}

// NewCronTicker returns a CronTicker struct.
// You can check the ticker channel for the next tick by
// `CronTicker.Chan()`.
func NewCronTicker(schedule string) (*CronTicker, error) {
	var cronTicker CronTicker
	var err error

	cronTicker.C = make(chan time.Time, 1)
	cronTicker.k = make(chan bool, 1)

	err = newCronTicker(schedule, cronTicker.C, cronTicker.k)
	if err != nil {
		return nil, err
	}
	return &cronTicker, nil
}

// newCronTicker prepares the channels, parses the schedule, and kicks off
// the goroutine that handles scheduling of each 'tick'.
func newCronTicker(schedule string, c chan time.Time, k <-chan bool) error {
	var err error

	scheduleWithTZ, loc, err := guaranteeTimeZone(schedule)
	if err != nil {
		return err
	}
	parser := getScheduleParser()

	cronSchedule, err := parser.Parse(scheduleWithTZ)
	if err != nil {
		return err
	}

	go cronRunner(cronSchedule, loc, c, k)

	return nil

}

// getScheduleParser returns a new parser that allows the use of the 'seconds' field
// like in the Quarts cron format, as well as descriptors such as '@weekly'.
func getScheduleParser() cron.Parser {
	parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return parser
}

// guaranteeTimeZone sets the `TZ=` value to `UTC` if there is none
// already in the cron schedule string.
func guaranteeTimeZone(schedule string) (string, *time.Location, error) {
	var loc *time.Location

	// If time zone is not included, set default to UTC
	if !strings.HasPrefix(schedule, "TZ=") {
		schedule = fmt.Sprintf("TZ=%s %s", "UTC", schedule)
	}

	tz := extractTZ(schedule)

	loc, err := time.LoadLocation(tz)
	if err != nil {
		return schedule, loc, err
	}

	return schedule, loc, nil
}

func extractTZ(schedule string) string {
	end := strings.Index(schedule, " ")
	eq := strings.Index(schedule, "=")
	return schedule[eq+1 : end]
}

// cronRunner handles calculating the next 'tick'. It communicates to
// the CronTicker via a channel and will stop/return whenever it receives
// a bool on the `k` channel.
func cronRunner(schedule cron.Schedule, loc *time.Location, c chan time.Time, k <-chan bool) {
	nextTick := schedule.Next(time.Now().In(loc))
	timer := time.NewTimer(time.Until(nextTick))
	for {
		select {
		case <-k:
			timer.Stop()
			return
		case tickTime := <-timer.C:
			c <- tickTime
			nextTick = schedule.Next(tickTime.In(loc))
			timer.Reset(time.Until(nextTick))
		}
	}
}
