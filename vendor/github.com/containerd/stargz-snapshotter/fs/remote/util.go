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

/*
   Copyright 2019 The Go Authors. All rights reserved.
   Use of this source code is governed by a BSD-style
   license that can be found in the NOTICE.md file.
*/

package remote

// region is HTTP-range-request-compliant range.
// "b" is beginning byte of the range and "e" is the end.
// "e" is must be inclusive along with HTTP's range expression.
type region struct{ b, e int64 }

func (c region) size() int64 {
	return c.e - c.b + 1
}

func superRegion(regs []region) region {
	s := regs[0]
	for _, reg := range regs {
		if reg.b < s.b {
			s.b = reg.b
		}
		if reg.e > s.e {
			s.e = reg.e
		}
	}
	return s
}

// regionSet is a set of regions
type regionSet struct {
	rs []region // must be kept sorted
}

// add attempts to merge r to rs.rs with squashing the regions as
// small as possible. This operation takes O(n).
// TODO: more efficient way to do it.
func (rs *regionSet) add(r region) {
	// Iterate over the sorted region slice from the tail.
	// a) When an overwrap occurs, adjust `r` to fully contain the looking region
	//    `l` and remove `l` from region slice.
	// b) Once l.e become less than r.b, no overwrap will occur again. So immediately
	//    insert `r` which fully contains all overwrapped regions, to the region slice.
	//    Here, `r` is inserted to the region slice with keeping it sorted, without
	//    overwrapping to any regions.
	// *) If any `l` contains `r`, we don't need to do anything so return immediately.
	for i := len(rs.rs) - 1; i >= 0; i-- {
		l := &rs.rs[i]

		// *) l contains r
		if l.b <= r.b && r.e <= l.e {
			return
		}

		// a) r overwraps to l so adjust r to fully contain l and reomve l
		//    from region slice.
		if l.b <= r.b && r.b <= l.e+1 && l.e <= r.e {
			r.b = l.b
			rs.rs = append(rs.rs[:i], rs.rs[i+1:]...)
			continue
		}
		if r.b <= l.b && l.b <= r.e+1 && r.e <= l.e {
			r.e = l.e
			rs.rs = append(rs.rs[:i], rs.rs[i+1:]...)
			continue
		}
		if r.b <= l.b && l.e <= r.e {
			rs.rs = append(rs.rs[:i], rs.rs[i+1:]...)
			continue
		}

		// b) No overwrap will occur after this iteration. Instert r to the
		//    region slice immediately.
		if l.e < r.b {
			rs.rs = append(rs.rs[:i+1], append([]region{r}, rs.rs[i+1:]...)...)
			return
		}

		// No overwrap occurs yet. See the next region.
	}

	// r is the topmost region among regions in the slice.
	rs.rs = append([]region{r}, rs.rs...)
}

func (rs *regionSet) totalSize() int64 {
	var sz int64
	for _, f := range rs.rs {
		sz += f.size()
	}
	return sz
}
