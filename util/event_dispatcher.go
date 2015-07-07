package util

import (
	"reflect"
	"sync"
)

// EventDispatcher is responsible for managing listeners for named events
// and dispatching event notifications to those listeners.
type EventDispatcher struct {
	sync.RWMutex
	source    interface{}
	listeners map[string]eventListeners
}

// EventListener is a function that can receive event notifications.
type EventListener func(Event)

// EventListeners represents a collection of individual listeners.
type eventListeners []EventListener

// newEventDispatcher creates a new EventDispatcher instance.
func NewEventDispatcher(source interface{}) *EventDispatcher {
	return &EventDispatcher{
		source:    source,
		listeners: make(map[string]eventListeners),
	}
}

// AddEventListener adds a listener function for a given event type.
func (d *EventDispatcher) AddEventListener(typ string, listener EventListener) {
	d.Lock()
	defer d.Unlock()
	d.listeners[typ] = append(d.listeners[typ], listener)
}

// RemoveEventListener removes a listener function for a given event type.
func (d *EventDispatcher) RemoveEventListener(typ string, listener EventListener) {
	d.Lock()
	defer d.Unlock()

	// Grab a reference to the function pointer once.
	ptr := reflect.ValueOf(listener).Pointer()

	// Find listener by pointer and remove it.
	listeners := d.listeners[typ]
	for i, l := range listeners {
		if reflect.ValueOf(l).Pointer() == ptr {
			d.listeners[typ] = append(listeners[:i], listeners[i+1:]...)
		}
	}
}

// DispatchEvent dispatches an event.
func (d *EventDispatcher) DispatchEvent(e Event) {
	d.RLock()
	defer d.RUnlock()

	// Automatically set the event source.
	if e, ok := e.(*event); ok {
		e.source = d.source
	}

	// Dispatch the event to all listeners.
	for _, l := range d.listeners[e.Type()] {
		l(e)
	}
}
