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
   license that can be found in the LICENSE file.
*/

package estargz

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/stargz-snapshotter/estargz/errorutil"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

// A Reader permits random access reads from a stargz file.
type Reader struct {
	sr        *io.SectionReader
	toc       *jtoc
	tocDigest digest.Digest

	// m stores all non-chunk entries, keyed by name.
	m map[string]*TOCEntry

	// chunks stores all TOCEntry values for regular files that
	// are split up. For a file with a single chunk, it's only
	// stored in m.
	chunks map[string][]*TOCEntry
}

// Open opens a stargz file for reading.
//
// Note that each entry name is normalized as the path that is relative to root.
func Open(sr *io.SectionReader) (*Reader, error) {
	tocOff, footerSize, err := OpenFooter(sr)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing footer")
	}
	tocTargz := make([]byte, sr.Size()-tocOff-footerSize)
	if _, err := sr.ReadAt(tocTargz, tocOff); err != nil {
		return nil, fmt.Errorf("error reading %d byte TOC targz: %v", len(tocTargz), err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(tocTargz))
	if err != nil {
		return nil, fmt.Errorf("malformed TOC gzip header: %v", err)
	}
	zr.Multistream(false)
	tr := tar.NewReader(zr)
	h, err := tr.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to find tar header in TOC gzip stream: %v", err)
	}
	if h.Name != TOCTarName {
		return nil, fmt.Errorf("TOC tar entry had name %q; expected %q", h.Name, TOCTarName)
	}
	dgstr := digest.Canonical.Digester()
	toc := new(jtoc)
	if err := json.NewDecoder(io.TeeReader(tr, dgstr.Hash())).Decode(&toc); err != nil {
		return nil, fmt.Errorf("error decoding TOC JSON: %v", err)
	}
	r := &Reader{sr: sr, toc: toc, tocDigest: dgstr.Digest()}
	if err := r.initFields(); err != nil {
		return nil, fmt.Errorf("failed to initialize fields of entries: %v", err)
	}
	return r, nil
}

// OpenFooter extracts and parses footer from the given blob.
func OpenFooter(sr *io.SectionReader) (tocOffset int64, footerSize int64, rErr error) {
	if sr.Size() < FooterSize && sr.Size() < legacyFooterSize {
		return 0, 0, fmt.Errorf("blob size %d is smaller than the footer size", sr.Size())
	}
	// TODO: read a bigger chunk (1MB?) at once here to hopefully
	// get the TOC + footer in one go.
	var footer [FooterSize]byte
	if _, err := sr.ReadAt(footer[:], sr.Size()-FooterSize); err != nil {
		return 0, 0, fmt.Errorf("error reading footer: %v", err)
	}
	return parseFooter(footer[:])
}

// initFields populates the Reader from r.toc after decoding it from
// JSON.
//
// Unexported fields are populated and TOCEntry fields that were
// implicit in the JSON are populated.
func (r *Reader) initFields() error {
	r.m = make(map[string]*TOCEntry, len(r.toc.Entries))
	r.chunks = make(map[string][]*TOCEntry)
	var lastPath string
	uname := map[int]string{}
	gname := map[int]string{}
	var lastRegEnt *TOCEntry
	for _, ent := range r.toc.Entries {
		ent.Name = cleanEntryName(ent.Name)
		if ent.Type == "reg" {
			lastRegEnt = ent
		}
		if ent.Type == "chunk" {
			ent.Name = lastPath
			r.chunks[ent.Name] = append(r.chunks[ent.Name], ent)
			if ent.ChunkSize == 0 && lastRegEnt != nil {
				ent.ChunkSize = lastRegEnt.Size - ent.ChunkOffset
			}
		} else {
			lastPath = ent.Name

			if ent.Uname != "" {
				uname[ent.UID] = ent.Uname
			} else {
				ent.Uname = uname[ent.UID]
			}
			if ent.Gname != "" {
				gname[ent.GID] = ent.Gname
			} else {
				ent.Gname = uname[ent.GID]
			}

			ent.modTime, _ = time.Parse(time.RFC3339, ent.ModTime3339)

			if ent.Type == "dir" {
				ent.NumLink++ // Parent dir links to this directory
			}
			r.m[ent.Name] = ent
		}
		if ent.Type == "reg" && ent.ChunkSize > 0 && ent.ChunkSize < ent.Size {
			r.chunks[ent.Name] = make([]*TOCEntry, 0, ent.Size/ent.ChunkSize+1)
			r.chunks[ent.Name] = append(r.chunks[ent.Name], ent)
		}
		if ent.ChunkSize == 0 && ent.Size != 0 {
			ent.ChunkSize = ent.Size
		}
	}

	// Populate children, add implicit directories:
	for _, ent := range r.toc.Entries {
		if ent.Type == "chunk" {
			continue
		}
		// add "foo/":
		//    add "foo" child to "" (creating "" if necessary)
		//
		// add "foo/bar/":
		//    add "bar" child to "foo" (creating "foo" if necessary)
		//
		// add "foo/bar.txt":
		//    add "bar.txt" child to "foo" (creating "foo" if necessary)
		//
		// add "a/b/c/d/e/f.txt":
		//    create "a/b/c/d/e" node
		//    add "f.txt" child to "e"

		name := ent.Name
		pdirName := parentDir(name)
		if name == pdirName {
			// This entry and its parent are the same.
			// Ignore this for avoiding infinite loop of the reference.
			// The example case where this can occur is when tar contains the root
			// directory itself (e.g. "./", "/").
			continue
		}
		pdir := r.getOrCreateDir(pdirName)
		ent.NumLink++ // at least one name(ent.Name) references this entry.
		if ent.Type == "hardlink" {
			if org, ok := r.m[cleanEntryName(ent.LinkName)]; ok {
				org.NumLink++ // original entry is referenced by this ent.Name.
				ent = org
			} else {
				return fmt.Errorf("%q is a hardlink but the linkname %q isn't found", ent.Name, ent.LinkName)
			}
		}
		pdir.addChild(path.Base(name), ent)
	}

	lastOffset := r.sr.Size()
	for i := len(r.toc.Entries) - 1; i >= 0; i-- {
		e := r.toc.Entries[i]
		if e.isDataType() {
			e.nextOffset = lastOffset
		}
		if e.Offset != 0 {
			lastOffset = e.Offset
		}
	}

	return nil
}

func parentDir(p string) string {
	dir, _ := path.Split(p)
	return strings.TrimSuffix(dir, "/")
}

func (r *Reader) getOrCreateDir(d string) *TOCEntry {
	e, ok := r.m[d]
	if !ok {
		e = &TOCEntry{
			Name:    d,
			Type:    "dir",
			Mode:    0755,
			NumLink: 2, // The directory itself(.) and the parent link to this directory.
		}
		r.m[d] = e
		if d != "" {
			pdir := r.getOrCreateDir(parentDir(d))
			pdir.addChild(path.Base(d), e)
		}
	}
	return e
}

// VerifyTOC checks that the TOC JSON in the passed blob matches the
// passed digests and that the TOC JSON contains digests for all chunks
// contained in the blob. If the verification succceeds, this function
// returns TOCEntryVerifier which holds all chunk digests in the stargz blob.
func (r *Reader) VerifyTOC(tocDigest digest.Digest) (TOCEntryVerifier, error) {
	// Verify the digest of TOC JSON
	if r.tocDigest != tocDigest {
		return nil, fmt.Errorf("invalid TOC JSON %q; want %q", r.tocDigest, tocDigest)
	}
	digestMap := make(map[int64]digest.Digest) // map from chunk offset to the digest
	for _, e := range r.toc.Entries {
		if e.Type == "reg" || e.Type == "chunk" {
			if e.Type == "reg" && e.Size == 0 {
				continue // ignores empty file
			}

			// offset must be unique in stargz blob
			if _, ok := digestMap[e.Offset]; ok {
				return nil, fmt.Errorf("offset %d found twice", e.Offset)
			}

			// all chunk entries must contain digest
			if e.ChunkDigest == "" {
				return nil, fmt.Errorf("ChunkDigest of %q(off=%d) not found in TOC JSON",
					e.Name, e.Offset)
			}

			d, err := digest.Parse(e.ChunkDigest)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse digest %q", e.ChunkDigest)
			}
			digestMap[e.Offset] = d
		}
	}

	return &verifier{digestMap: digestMap}, nil
}

// verifier is an implementation of TOCEntryVerifier which holds verifiers keyed by
// offset of the chunk.
type verifier struct {
	digestMap   map[int64]digest.Digest
	digestMapMu sync.Mutex
}

// Verifier returns a content verifier specified by TOCEntry.
func (v *verifier) Verifier(ce *TOCEntry) (digest.Verifier, error) {
	v.digestMapMu.Lock()
	defer v.digestMapMu.Unlock()
	d, ok := v.digestMap[ce.Offset]
	if !ok {
		return nil, fmt.Errorf("verifier for offset=%d,size=%d hasn't been registered",
			ce.Offset, ce.ChunkSize)
	}
	return d.Verifier(), nil
}

// ChunkEntryForOffset returns the TOCEntry containing the byte of the
// named file at the given offset within the file.
// Name must be absolute path or one that is relative to root.
func (r *Reader) ChunkEntryForOffset(name string, offset int64) (e *TOCEntry, ok bool) {
	name = cleanEntryName(name)
	e, ok = r.Lookup(name)
	if !ok || !e.isDataType() {
		return nil, false
	}
	ents := r.chunks[name]
	if len(ents) < 2 {
		if offset >= e.ChunkSize {
			return nil, false
		}
		return e, true
	}
	i := sort.Search(len(ents), func(i int) bool {
		e := ents[i]
		return e.ChunkOffset >= offset || (offset > e.ChunkOffset && offset < e.ChunkOffset+e.ChunkSize)
	})
	if i == len(ents) {
		return nil, false
	}
	return ents[i], true
}

// Lookup returns the Table of Contents entry for the given path.
//
// To get the root directory, use the empty string.
// Path must be absolute path or one that is relative to root.
func (r *Reader) Lookup(path string) (e *TOCEntry, ok bool) {
	path = cleanEntryName(path)
	if r == nil {
		return
	}
	e, ok = r.m[path]
	if ok && e.Type == "hardlink" {
		e, ok = r.m[e.LinkName]
	}
	return
}

// OpenFile returns the reader of the specified file payload.
//
// Name must be absolute path or one that is relative to root.
func (r *Reader) OpenFile(name string) (*io.SectionReader, error) {
	name = cleanEntryName(name)
	ent, ok := r.Lookup(name)
	if !ok {
		// TODO: come up with some error plan. This is lazy:
		return nil, &os.PathError{
			Path: name,
			Op:   "OpenFile",
			Err:  os.ErrNotExist,
		}
	}
	if ent.Type != "reg" {
		return nil, &os.PathError{
			Path: name,
			Op:   "OpenFile",
			Err:  errors.New("not a regular file"),
		}
	}
	fr := &fileReader{
		r:    r,
		size: ent.Size,
		ents: r.getChunks(ent),
	}
	return io.NewSectionReader(fr, 0, fr.size), nil
}

func (r *Reader) getChunks(ent *TOCEntry) []*TOCEntry {
	if ents, ok := r.chunks[ent.Name]; ok {
		return ents
	}
	return []*TOCEntry{ent}
}

type fileReader struct {
	r    *Reader
	size int64
	ents []*TOCEntry // 1 or more reg/chunk entries
}

func (fr *fileReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= fr.size {
		return 0, io.EOF
	}
	if off < 0 {
		return 0, errors.New("invalid offset")
	}
	var i int
	if len(fr.ents) > 1 {
		i = sort.Search(len(fr.ents), func(i int) bool {
			return fr.ents[i].ChunkOffset >= off
		})
		if i == len(fr.ents) {
			i = len(fr.ents) - 1
		}
	}
	ent := fr.ents[i]
	if ent.ChunkOffset > off {
		if i == 0 {
			return 0, errors.New("internal error; first chunk offset is non-zero")
		}
		ent = fr.ents[i-1]
	}

	//  If ent is a chunk of a large file, adjust the ReadAt
	//  offset by the chunk's offset.
	off -= ent.ChunkOffset

	finalEnt := fr.ents[len(fr.ents)-1]
	gzOff := ent.Offset
	// gzBytesRemain is the number of compressed gzip bytes in this
	// file remaining, over 1+ gzip chunks.
	gzBytesRemain := finalEnt.NextOffset() - gzOff

	sr := io.NewSectionReader(fr.r.sr, gzOff, gzBytesRemain)

	const maxGZread = 2 << 20
	var bufSize = maxGZread
	if gzBytesRemain < maxGZread {
		bufSize = int(gzBytesRemain)
	}

	br := bufio.NewReaderSize(sr, bufSize)
	if _, err := br.Peek(bufSize); err != nil {
		return 0, fmt.Errorf("fileReader.ReadAt.peek: %v", err)
	}

	gz, err := gzip.NewReader(br)
	if err != nil {
		return 0, fmt.Errorf("fileReader.ReadAt.gzipNewReader: %v", err)
	}
	if n, err := io.CopyN(ioutil.Discard, gz, off); n != off || err != nil {
		return 0, fmt.Errorf("discard of %d bytes = %v, %v", off, n, err)
	}
	return io.ReadFull(gz, p)
}

// A Writer writes stargz files.
//
// Use NewWriter to create a new Writer.
type Writer struct {
	bw       *bufio.Writer
	cw       *countWriter
	toc      *jtoc
	diffHash hash.Hash // SHA-256 of uncompressed tar

	closed           bool
	gz               *gzip.Writer
	lastUsername     map[int]string
	lastGroupname    map[int]string
	compressionLevel int

	// ChunkSize optionally controls the maximum number of bytes
	// of data of a regular file that can be written in one gzip
	// stream before a new gzip stream is started.
	// Zero means to use a default, currently 4 MiB.
	ChunkSize int
}

// currentGzipWriter writes to the current w.gz field, which can
// change throughout writing a tar entry.
//
// Additionally, it updates w's SHA-256 of the uncompressed bytes
// of the tar file.
type currentGzipWriter struct{ w *Writer }

func (cgw currentGzipWriter) Write(p []byte) (int, error) {
	cgw.w.diffHash.Write(p)
	return cgw.w.gz.Write(p)
}

func (w *Writer) chunkSize() int {
	if w.ChunkSize <= 0 {
		return 4 << 20
	}
	return w.ChunkSize
}

// NewWriter returns a new stargz writer writing to w.
//
// The writer must be closed to write its trailing table of contents.
func NewWriter(w io.Writer) *Writer {
	return NewWriterLevel(w, gzip.BestCompression)
}

// NewWriterLevel returns a new stargz writer writing to w.
// The compression level is configurable.
//
// The writer must be closed to write its trailing table of contents.
func NewWriterLevel(w io.Writer, compressionLevel int) *Writer {
	bw := bufio.NewWriter(w)
	cw := &countWriter{w: bw}
	return &Writer{
		bw:               bw,
		cw:               cw,
		toc:              &jtoc{Version: 1},
		diffHash:         sha256.New(),
		compressionLevel: compressionLevel,
	}
}

// Close writes the stargz's table of contents and flushes all the
// buffers, returning any error.
func (w *Writer) Close() (digest.Digest, error) {
	if w.closed {
		return "", nil
	}
	defer func() { w.closed = true }()

	if err := w.closeGz(); err != nil {
		return "", err
	}

	// Write the TOC index.
	tocOff := w.cw.n
	w.gz, _ = gzip.NewWriterLevel(w.cw, w.compressionLevel)
	tw := tar.NewWriter(currentGzipWriter{w})
	tocJSON, err := json.MarshalIndent(w.toc, "", "\t")
	if err != nil {
		return "", err
	}
	if err := tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     TOCTarName,
		Size:     int64(len(tocJSON)),
	}); err != nil {
		return "", err
	}
	if _, err := tw.Write(tocJSON); err != nil {
		return "", err
	}

	if err := tw.Close(); err != nil {
		return "", err
	}
	if err := w.closeGz(); err != nil {
		return "", err
	}

	// And a little footer with pointer to the TOC gzip stream.
	if _, err := w.bw.Write(footerBytes(tocOff)); err != nil {
		return "", err
	}

	if err := w.bw.Flush(); err != nil {
		return "", err
	}

	return digest.FromBytes(tocJSON), nil
}

func (w *Writer) closeGz() error {
	if w.closed {
		return errors.New("write on closed Writer")
	}
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			return err
		}
		w.gz = nil
	}
	return nil
}

// nameIfChanged returns name, unless it was the already the value of (*mp)[id],
// in which case it returns the empty string.
func (w *Writer) nameIfChanged(mp *map[int]string, id int, name string) string {
	if name == "" {
		return ""
	}
	if *mp == nil {
		*mp = make(map[int]string)
	}
	if (*mp)[id] == name {
		return ""
	}
	(*mp)[id] = name
	return name
}

func (w *Writer) condOpenGz() {
	if w.gz == nil {
		w.gz, _ = gzip.NewWriterLevel(w.cw, w.compressionLevel)
	}
}

// AppendTar reads the tar or tar.gz file from r and appends
// each of its contents to w.
//
// The input r can optionally be gzip compressed but the output will
// always be gzip compressed.
func (w *Writer) AppendTar(r io.Reader) error {
	br := bufio.NewReader(r)
	var tr *tar.Reader
	if isGzip(br) {
		// NewReader can't fail if isGzip returned true.
		zr, _ := gzip.NewReader(br)
		tr = tar.NewReader(zr)
	} else {
		tr = tar.NewReader(br)
	}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading from source tar: tar.Reader.Next: %v", err)
		}
		if h.Name == TOCTarName {
			// It is possible for a layer to be "stargzified" twice during the
			// distribution lifecycle. So we reserve "TOCTarName" here to avoid
			// duplicated entries in the resulting layer.
			continue
		}

		xattrs := make(map[string][]byte)
		const xattrPAXRecordsPrefix = "SCHILY.xattr."
		if h.PAXRecords != nil {
			for k, v := range h.PAXRecords {
				if strings.HasPrefix(k, xattrPAXRecordsPrefix) {
					xattrs[k[len(xattrPAXRecordsPrefix):]] = []byte(v)
				}
			}
		}
		ent := &TOCEntry{
			Name:        h.Name,
			Mode:        h.Mode,
			UID:         h.Uid,
			GID:         h.Gid,
			Uname:       w.nameIfChanged(&w.lastUsername, h.Uid, h.Uname),
			Gname:       w.nameIfChanged(&w.lastGroupname, h.Gid, h.Gname),
			ModTime3339: formatModtime(h.ModTime),
			Xattrs:      xattrs,
		}
		w.condOpenGz()
		tw := tar.NewWriter(currentGzipWriter{w})
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeLink:
			ent.Type = "hardlink"
			ent.LinkName = h.Linkname
		case tar.TypeSymlink:
			ent.Type = "symlink"
			ent.LinkName = h.Linkname
		case tar.TypeDir:
			ent.Type = "dir"
		case tar.TypeReg:
			ent.Type = "reg"
			ent.Size = h.Size
		case tar.TypeChar:
			ent.Type = "char"
			ent.DevMajor = int(h.Devmajor)
			ent.DevMinor = int(h.Devminor)
		case tar.TypeBlock:
			ent.Type = "block"
			ent.DevMajor = int(h.Devmajor)
			ent.DevMinor = int(h.Devminor)
		case tar.TypeFifo:
			ent.Type = "fifo"
		default:
			return fmt.Errorf("unsupported input tar entry %q", h.Typeflag)
		}

		// We need to keep a reference to the TOC entry for regular files, so that we
		// can fill the digest later.
		var regFileEntry *TOCEntry
		var payloadDigest digest.Digester
		if h.Typeflag == tar.TypeReg {
			regFileEntry = ent
			payloadDigest = digest.Canonical.Digester()
		}

		if h.Typeflag == tar.TypeReg && ent.Size > 0 {
			var written int64
			totalSize := ent.Size // save it before we destroy ent
			tee := io.TeeReader(tr, payloadDigest.Hash())
			for written < totalSize {
				if err := w.closeGz(); err != nil {
					return err
				}

				chunkSize := int64(w.chunkSize())
				remain := totalSize - written
				if remain < chunkSize {
					chunkSize = remain
				} else {
					ent.ChunkSize = chunkSize
				}
				ent.Offset = w.cw.n
				ent.ChunkOffset = written
				chunkDigest := digest.Canonical.Digester()

				w.condOpenGz()

				teeChunk := io.TeeReader(tee, chunkDigest.Hash())
				if _, err := io.CopyN(tw, teeChunk, chunkSize); err != nil {
					return fmt.Errorf("error copying %q: %v", h.Name, err)
				}
				ent.ChunkDigest = chunkDigest.Digest().String()
				w.toc.Entries = append(w.toc.Entries, ent)
				written += chunkSize
				ent = &TOCEntry{
					Name: h.Name,
					Type: "chunk",
				}
			}
		} else {
			w.toc.Entries = append(w.toc.Entries, ent)
		}
		if payloadDigest != nil {
			regFileEntry.Digest = payloadDigest.Digest().String()
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// DiffID returns the SHA-256 of the uncompressed tar bytes.
// It is only valid to call DiffID after Close.
func (w *Writer) DiffID() string {
	return fmt.Sprintf("sha256:%x", w.diffHash.Sum(nil))
}

// footerBytes returns the 51 bytes footer.
func footerBytes(tocOff int64) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, FooterSize))
	gz, _ := gzip.NewWriterLevel(buf, gzip.NoCompression) // MUST be NoCompression to keep 51 bytes

	// Extra header indicating the offset of TOCJSON
	// https://tools.ietf.org/html/rfc1952#section-2.3.1.1
	header := make([]byte, 4)
	header[0], header[1] = 'S', 'G'
	subfield := fmt.Sprintf("%016xSTARGZ", tocOff)
	binary.LittleEndian.PutUint16(header[2:4], uint16(len(subfield))) // little-endian per RFC1952
	gz.Header.Extra = append(header, []byte(subfield)...)
	gz.Close()
	if buf.Len() != FooterSize {
		panic(fmt.Sprintf("footer buffer = %d, not %d", buf.Len(), FooterSize))
	}
	return buf.Bytes()
}

func parseFooter(p []byte) (tocOffset int64, footerSize int64, rErr error) {
	var allErr []error

	tocOffset, err := parseEStargzFooter(p)
	if err == nil {
		return tocOffset, FooterSize, nil
	}
	allErr = append(allErr, err)

	pad := len(p) - legacyFooterSize
	if pad < 0 {
		pad = 0
	}
	tocOffset, err = parseLegacyFooter(p[pad:])
	if err == nil {
		return tocOffset, legacyFooterSize, nil
	}
	return 0, 0, errorutil.Aggregate(append(allErr, err))
}

func parseEStargzFooter(p []byte) (tocOffset int64, err error) {
	if len(p) != FooterSize {
		return 0, fmt.Errorf("invalid length %d cannot be parsed", len(p))
	}
	zr, err := gzip.NewReader(bytes.NewReader(p))
	if err != nil {
		return 0, err
	}
	extra := zr.Header.Extra
	si1, si2, subfieldlen, subfield := extra[0], extra[1], extra[2:4], extra[4:]
	if si1 != 'S' || si2 != 'G' {
		return 0, fmt.Errorf("invalid subfield IDs: %q, %q; want E, S", si1, si2)
	}
	if slen := binary.LittleEndian.Uint16(subfieldlen); slen != uint16(16+len("STARGZ")) {
		return 0, fmt.Errorf("invalid length of subfield %d; want %d", slen, 16+len("STARGZ"))
	}
	if string(subfield[16:]) != "STARGZ" {
		return 0, fmt.Errorf("STARGZ magic string must be included in the footer subfield")
	}
	return strconv.ParseInt(string(subfield[:16]), 16, 64)
}

func parseLegacyFooter(p []byte) (tocOffset int64, err error) {
	if len(p) != legacyFooterSize {
		return 0, fmt.Errorf("legacy: invalid length %d cannot be parsed", len(p))
	}
	zr, err := gzip.NewReader(bytes.NewReader(p))
	if err != nil {
		return 0, errors.Wrapf(err, "legacy: failed to get footer gzip reader")
	}
	extra := zr.Header.Extra
	if len(extra) != 16+len("STARGZ") {
		return 0, fmt.Errorf("legacy: invalid stargz's extra field size")
	}
	if string(extra[16:]) != "STARGZ" {
		return 0, fmt.Errorf("legacy: magic string STARGZ not found")
	}
	return strconv.ParseInt(string(extra[:16]), 16, 64)
}

func formatModtime(t time.Time) string {
	if t.IsZero() || t.Unix() == 0 {
		return ""
	}
	return t.UTC().Round(time.Second).Format(time.RFC3339)
}

func cleanEntryName(name string) string {
	// Use path.Clean to consistently deal with path separators across platforms.
	return strings.TrimPrefix(path.Clean("/"+name), "/")
}

// countWriter counts how many bytes have been written to its wrapped
// io.Writer.
type countWriter struct {
	w io.Writer
	n int64
}

func (cw *countWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.n += int64(n)
	return
}

// isGzip reports whether br is positioned right before an upcoming gzip stream.
// It does not consume any bytes from br.
func isGzip(br *bufio.Reader) bool {
	const (
		gzipID1     = 0x1f
		gzipID2     = 0x8b
		gzipDeflate = 8
	)
	peek, _ := br.Peek(3)
	return len(peek) >= 3 && peek[0] == gzipID1 && peek[1] == gzipID2 && peek[2] == gzipDeflate
}
