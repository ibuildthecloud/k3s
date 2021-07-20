/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// Package namedmutex provides NamedMutex that wraps sync.Mutex
// and provides namespaced mutex.
package namedmutex

import (
	"sync"
)

// NamedMutex wraps sync.Mutex and provides namespaced mutex.
type NamedMutex struct {
	muMap  map[string]*sync.Mutex
	refMap map[string]int

	mu sync.Mutex
}

// Lock locks the mutex of the given name
func (nl *NamedMutex) Lock(name string) {
	nl.mu.Lock()
	if nl.muMap == nil {
		nl.muMap = make(map[string]*sync.Mutex)
	}
	if nl.refMap == nil {
		nl.refMap = make(map[string]int)
	}
	if _, ok := nl.muMap[name]; !ok {
		nl.muMap[name] = &sync.Mutex{}
	}
	mu := nl.muMap[name]
	nl.refMap[name]++
	nl.mu.Unlock()
	mu.Lock()
}

// Unlock unlocks the mutex of the given name
func (nl *NamedMutex) Unlock(name string) {
	nl.mu.Lock()
	mu := nl.muMap[name]
	nl.refMap[name]--
	if nl.refMap[name] <= 0 {
		delete(nl.muMap, name)
		delete(nl.refMap, name)
	}
	nl.mu.Unlock()
	mu.Unlock()
}
