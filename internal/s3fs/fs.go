package s3fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"golang.org/x/net/webdav"
)

type Options struct {
	// UploadBufferLimit defines the maximum in-memory buffer used for writes before upload.
	// For small files (50-300KB) this is safe; default is 8 MiB (configured by caller).
	UploadBufferLimit int64
}

type FS struct {
	cli  *minio.Client
	bkt  string
	opts Options
}

func New(cli *minio.Client, bucket string, opts Options) *FS {
	return &FS{cli: cli, bkt: bucket, opts: opts}
}

// Ensure FS implements webdav.FileSystem
var _ webdav.FileSystem = (*FS)(nil)

func (f *FS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	key := normalizeKey(name)
	if key == "" {
		return nil // root exists
	}
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	// Create a zero-byte marker for the directory
	_, err := f.cli.PutObject(ctx, f.bkt, key, bytes.NewReader(nil), 0, minio.PutObjectOptions{
		ContentType: "application/x-directory",
	})
	return err
}

func (f *FS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	key := normalizeKey(name)

	// Disallow append: Not supported efficiently on S3
	if flag&os.O_APPEND != 0 {
		return nil, toPathError("open", name, fmt.Errorf("append not supported"))
	}

	// Remember if the request explicitly addressed a directory (trailing slash)
	hadSlash := strings.HasSuffix(key, "/")

	// Disallow writes to directory paths
	if hadSlash && (flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0) {
		return nil, toPathError("open", name, os.ErrInvalid)
	}

	// Normalize directory suffix away for internal checks
	trimKey := strings.TrimSuffix(key, "/")

	// If writable requested -> create a write-only handle (buffered)
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		w := &s3File{
			fs:       f,
			key:      trimKey,
			writable: true,
			buf:      bytes.NewBuffer(nil),
			bufLimit: f.opts.UploadBufferLimit,
		}
		return w, nil
	}

	// Read-only open
	// Root directory always exists -> return directory handle
	if trimKey == "" {
		return &s3Dir{fs: f, key: ""}, nil
	}

	// If the path explicitly ends with '/', treat it strictly as a directory,
	// and do not attempt to open an object with the same name.
	if hadSlash {
		dirMarker := trimKey + "/"
		if _, derr := f.cli.StatObject(ctx, f.bkt, dirMarker, minio.StatObjectOptions{}); derr == nil {
			return &s3Dir{fs: f, key: trimKey}, nil
		}
		for o := range f.cli.ListObjects(ctx, f.bkt, minio.ListObjectsOptions{
			Prefix:    dirMarker,
			Recursive: false,
		}) {
			if o.Err != nil {
				return nil, toPathError("open", name, o.Err)
			}
			return &s3Dir{fs: f, key: trimKey}, nil
		}
		return nil, toPathError("open", name, os.ErrNotExist)
	}

	// Try open as an object (file) first for non-slashed paths
	obj, err := f.cli.GetObject(ctx, f.bkt, trimKey, minio.GetObjectOptions{})
	if err == nil {
		if st, statErr := obj.Stat(); statErr == nil {
			return &s3File{
				fs:      f,
				key:     trimKey,
				ro:      obj,
				size:    st.Size,
				modTime: st.LastModified,
			}, nil
		}
		// If stat failed, close and proceed to try directory detection below
		_ = obj.Close()
	}

	// Not a regular object; try to detect a directory:
	// 1) Check for explicit directory marker "<key>/"
	dirMarker := trimKey + "/"
	if _, derr := f.cli.StatObject(ctx, f.bkt, dirMarker, minio.StatObjectOptions{}); derr == nil {
		return &s3Dir{fs: f, key: trimKey}, nil
	}
	// 2) Look for any object or common prefix under "<key>/" (non-recursive, faster)
	for o := range f.cli.ListObjects(ctx, f.bkt, minio.ListObjectsOptions{
		Prefix:    dirMarker,
		Recursive: false,
	}) {
		if o.Err != nil {
			return nil, toPathError("open", name, o.Err)
		}
		// Found at least one immediate child or sub-prefix -> it's a directory
		return &s3Dir{fs: f, key: trimKey}, nil
	}

	// Nothing found
	return nil, toPathError("open", name, os.ErrNotExist)
}

func (f *FS) RemoveAll(ctx context.Context, name string) error {
	key := normalizeKey(name)

	// Root: refuse destructive delete
	if key == "" {
		return fmt.Errorf("refuse to delete root")
	}

	// First try remove as a single object
	err := f.cli.RemoveObject(ctx, f.bkt, key, minio.RemoveObjectOptions{})
	if err == nil {
		// Also attempt to delete possible folder marker
		_ = f.cli.RemoveObject(ctx, f.bkt, key+"/", minio.RemoveObjectOptions{})
		return nil
	}

	// If not found as object, treat as directory prefix
	prefix := key
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	ch := make(chan minio.ObjectInfo, 1000)
	go func() {
		defer close(ch)
		for object := range f.cli.ListObjects(ctx, f.bkt, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			ch <- object
		}
	}()

	// Batch delete
	for obj := range ch {
		_ = f.cli.RemoveObject(ctx, f.bkt, obj.Key, minio.RemoveObjectOptions{})
	}
	// Also delete the directory marker if exists
	_ = f.cli.RemoveObject(ctx, f.bkt, prefix, minio.RemoveObjectOptions{})
	return nil
}

func (f *FS) Rename(ctx context.Context, oldName, newName string) error {
	src := normalizeKey(oldName)
	dst := normalizeKey(newName)
	if src == dst {
		return nil
	}
	// File-to-file rename if source exists as object
	if err := f.copyThenDelete(ctx, src, dst); err == nil {
		// cleanup dir markers
		_ = f.cli.RemoveObject(ctx, f.bkt, src+"/", minio.RemoveObjectOptions{})
		return nil
	}

	// Treat as directory rename
	if !strings.HasSuffix(src, "/") {
		src += "/"
	}
	if !strings.HasSuffix(dst, "/") {
		dst += "/"
	}

	// Iterate all objects under src and copy to dst
	var toDelete []minio.ObjectInfo
	for obj := range f.cli.ListObjects(ctx, f.bkt, minio.ListObjectsOptions{
		Prefix:    src,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return obj.Err
		}
		newKey := dst + strings.TrimPrefix(obj.Key, src)
		_, err := f.cli.CopyObject(ctx, minio.CopyDestOptions{
			Bucket: f.bkt,
			Object: newKey,
		}, minio.CopySrcOptions{
			Bucket: f.bkt,
			Object: obj.Key,
		})
		if err != nil {
			return err
		}
		toDelete = append(toDelete, obj)
	}

	// Delete originals
	for _, obj := range toDelete {
		_ = f.cli.RemoveObject(ctx, f.bkt, obj.Key, minio.RemoveObjectOptions{})
	}
	// Move marker too
	_, _ = f.cli.PutObject(ctx, f.bkt, dst, bytes.NewReader(nil), 0, minio.PutObjectOptions{
		ContentType: "application/x-directory",
	})
	_ = f.cli.RemoveObject(ctx, f.bkt, src, minio.RemoveObjectOptions{})
	return nil
}

func (f *FS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	key := normalizeKey(name)
	// Root
	if key == "" {
		return &fi{name: "/", size: 0, mode: os.ModeDir | 0o755, mod: time.Now(), dir: true}, nil
	}

	// Try file stat
	objInfo, err := f.cli.StatObject(ctx, f.bkt, key, minio.StatObjectOptions{})
	if err == nil {
		return &fi{
			name: path.Base(key),
			size: objInfo.Size,
			mode: 0o644,
			mod:  objInfo.LastModified,
			dir:  false,
		}, nil
	}

	// Try directory existence:
	// 1) Explicit directory marker "<key>/"
	prefix := key
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if mInfo, mErr := f.cli.StatObject(ctx, f.bkt, prefix, minio.StatObjectOptions{}); mErr == nil {
		return &fi{
			name: path.Base(strings.TrimSuffix(prefix, "/")),
			size: 0,
			mode: os.ModeDir | 0o755,
			mod:  mInfo.LastModified,
			dir:  true,
		}, nil
	}
	// 2) Any immediate child under the prefix (non-recursive listing)
	it := f.cli.ListObjects(ctx, f.bkt, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})
	for obj := range it {
		if obj.Err != nil {
			return nil, toPathError("stat", name, obj.Err)
		}
		// Presence of any object under the prefix indicates a directory
		return &fi{
			name: path.Base(strings.TrimSuffix(prefix, "/")),
			size: 0,
			mode: os.ModeDir | 0o755,
			mod:  obj.LastModified,
			dir:  true,
		}, nil
	}

	return nil, toPathError("stat", name, os.ErrNotExist)
}

func (f *FS) copyThenDelete(ctx context.Context, src, dst string) error {
	// Attempt to copy src (object) to dst
	_, err := f.cli.CopyObject(ctx, minio.CopyDestOptions{
		Bucket: f.bkt,
		Object: dst,
	}, minio.CopySrcOptions{
		Bucket: f.bkt,
		Object: src,
	})
	if err != nil {
		return err
	}
	return f.cli.RemoveObject(ctx, f.bkt, src, minio.RemoveObjectOptions{})
}

type s3File struct {
	mu     sync.Mutex
	closed bool
	fs     *FS
	key    string

	// read-only
	ro      *minio.Object
	size    int64
	modTime time.Time

	// write-only
	writable bool
	buf      *bytes.Buffer
	bufLimit int64
	once     sync.Once
}

var _ webdav.File = (*s3File)(nil)

func (f *s3File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true

	if f.writable {
		data := f.buf.Bytes()
		reader := bytes.NewReader(data)
		_, err := f.fs.cli.PutObject(context.Background(), f.fs.bkt, f.key, reader, int64(len(data)), minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			return err
		}
	}
	if f.ro != nil {
		return f.ro.Close()
	}
	return nil
}

func (f *s3File) Read(p []byte) (int, error) {
	if f.ro == nil {
		return 0, os.ErrInvalid
	}
	return f.ro.Read(p)
}

func (f *s3File) Seek(offset int64, whence int) (int64, error) {
	if f.ro == nil {
		// directory or write-only file
		if f.writable && offset == 0 && whence == io.SeekStart {
			return 0, nil
		}
		return 0, os.ErrInvalid
	}
	return f.ro.Seek(offset, whence)
}

func (f *s3File) Write(p []byte) (int, error) {
	if !f.writable {
		return 0, os.ErrInvalid
	}
	// Buffer in memory; for small files this avoids multipart and reduces S3 calls
	if f.bufLimit > 0 && int64(f.buf.Len()+len(p)) > f.bufLimit {
		return 0, fmt.Errorf("write exceeds buffer limit (%d bytes)", f.bufLimit)
	}
	return f.buf.Write(p)
}

func (f *s3File) Readdir(count int) ([]os.FileInfo, error) {
	// Files do not support Readdir
	return nil, os.ErrInvalid
}

func (f *s3File) Stat() (os.FileInfo, error) {
	if f.writable {
		return &fi{
			name: path.Base(f.key),
			size: int64(f.buf.Len()),
			mode: 0o644,
			mod:  time.Now(),
			dir:  false,
		}, nil
	}
	return &fi{
		name: path.Base(f.key),
		size: f.size,
		mode: 0o644,
		mod:  f.modTime,
		dir:  false,
	}, nil
}

// s3Dir implements a directory handle for WebDAV directory listings.
type s3Dir struct {
	fs    *FS
	key   string // normalized prefix with trailing '/'
	once  sync.Once
	ents  []os.FileInfo
	index int
	err   error
}

var _ webdav.File = (*s3Dir)(nil)

func (d *s3Dir) ensureListed() {
	d.once.Do(func() {
		ctx := context.Background()
		prefix := d.key
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		type void struct{}
		dirSet := make(map[string]void)
		fileMap := make(map[string]*fi)

		// Non-recursive listing; derive immediate children. Treat any "a/b" as directory "a".
		for obj := range d.fs.cli.ListObjects(ctx, d.fs.bkt, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: false,
		}) {
			if obj.Err != nil {
				d.err = obj.Err
				return
			}
			rel := strings.TrimPrefix(obj.Key, prefix)
			if rel == "" {
				// folder marker equals to prefix itself
				continue
			}
			// If there's a slash in rel, it's under a subdir -> record that subdir as an immediate child
			if i := strings.IndexByte(rel, '/'); i >= 0 {
				dirName := rel[:i]
				if dirName != "" {
					dirSet[dirName] = void{}
				}
				continue
			}
			// Explicit folder marker "name/" also indicates a directory
			if strings.HasSuffix(rel, "/") {
				dirName := strings.TrimSuffix(rel, "/")
				if dirName != "" {
					dirSet[dirName] = void{}
				}
				continue
			}
			// Immediate file
			fileMap[rel] = &fi{
				name: rel,
				size: obj.Size,
				mode: 0o644,
				mod:  obj.LastModified,
				dir:  false,
			}
		}

		// Build entries: sort for deterministic order
		for name := range dirSet {
			d.ents = append(d.ents, &fi{
				name: name,
				size: 0,
				mode: os.ModeDir | 0o755,
				mod:  time.Now(),
				dir:  true,
			})
		}
		for _, v := range fileMap {
			d.ents = append(d.ents, v)
		}
		sort.Slice(d.ents, func(i, j int) bool { return d.ents[i].Name() < d.ents[j].Name() })
	})
}

func (d *s3Dir) Close() error { return nil }

func (d *s3Dir) Read(p []byte) (int, error) { return 0, io.EOF }

func (d *s3Dir) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekStart {
		d.index = 0
		return 0, nil
	}
	return 0, os.ErrInvalid
}

func (d *s3Dir) Write(p []byte) (int, error) { return 0, os.ErrInvalid }

func (d *s3Dir) Readdir(count int) ([]os.FileInfo, error) {
	d.ensureListed()
	if d.err != nil {
		return nil, d.err
	}
	remaining := len(d.ents) - d.index

	// n <= 0: return all remaining entries and nil (even if empty)
	if count <= 0 {
		if remaining <= 0 {
			return []os.FileInfo{}, nil
		}
		res := d.ents[d.index:]
		d.index = len(d.ents)
		return res, nil
	}

	// n > 0: return up to count; if fewer than requested remain, return io.EOF with the last batch
	if remaining <= 0 {
		return nil, io.EOF
	}
	n := count
	if n > remaining {
		n = remaining
	}
	res := d.ents[d.index : d.index+n]
	d.index += n
	if n < count {
		return res, io.EOF
	}
	return res, nil
}

func (d *s3Dir) Stat() (os.FileInfo, error) {
	name := path.Base(strings.TrimSuffix(d.key, "/"))
	if d.key == "" {
		name = "/"
	}
	return &fi{
		name: name,
		size: 0,
		mode: os.ModeDir | 0o755,
		mod:  time.Now(),
		dir:  true,
	}, nil
}

// helper types and funcs

type fi struct {
	name string
	size int64
	mode os.FileMode
	mod  time.Time
	dir  bool
}

func (f *fi) Name() string       { return f.name }
func (f *fi) Size() int64        { return f.size }
func (f *fi) Mode() os.FileMode  { return f.mode }
func (f *fi) ModTime() time.Time { return f.mod }
func (f *fi) IsDir() bool        { return f.dir }
func (f *fi) Sys() any           { return nil }

func normalizeKey(p string) string {
	// WebDAV provides path with leading '/'; remove it and clean
	p = path.Clean("/" + p) // ensure leading slash for Clean
	p = strings.TrimPrefix(p, "/")
	if p == "." {
		return ""
	}
	return p
}

func toPathError(op, name string, err error) error {
	var pErr *os.PathError
	if errors.As(err, &pErr) {
		return pErr
	}
	return &os.PathError{Op: op, Path: name, Err: err}
}
