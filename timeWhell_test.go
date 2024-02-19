package timewheel

import (
	"testing"
	"time"
)

func Test_timeWheel(t *testing.T) {
	timeWheel := NewTimeWheel(10, 500*time.Millisecond)
	defer timeWheel.Stop()

	timeWheel.AddTask("test1", func() {
		t.Errorf("test1, %v", time.Now())
	}, time.Now().Add(time.Second))
	timeWheel.AddTask("test2", func() {
		t.Errorf("test2, %v", time.Now())
	}, time.Now().Add(5*time.Second))
	timeWheel.AddTask("test2", func() {
		t.Errorf("test2, %v", time.Now())
	}, time.Now().Add(3*time.Second))

	<-time.After(10 * time.Second)
}
