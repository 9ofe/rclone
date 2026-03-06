package imagewrap

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
)

const (
	basePngBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII="
	magicHeader   = "RCIMG001"
)

// Register with Fs
func init() {
	fsi := &fs.RegInfo{
		Name:        "imagewrap",
		Description: "Disguises files as PNGs",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "remote",
			Help:     "Remote or path to wrap.\n\nCan be \"myremote:path/to/dir\", \"myremote:bucket\", \"myremote:\" or \"/local/path\".",
			Required: true,
		}, {
			Name:     "base_png",
			Help:     "Base PNG configuration. Currently only 'embedded' is supported.",
			Default:  "embedded",
			Required: false,
		}},
	}
	fs.Register(fsi)
}

// Options defines the configuration for this backend
type Options struct {
	Remote  string `config:"remote"`
	BasePng string `config:"base_png"`
}

// Fs represents a wrapped remote
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	base     fs.Fs
	basePng  []byte
}

// NewFs constructs an Fs from the path.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.Remote == "" {
		return nil, errors.New("imagewrap can't point to an empty remote - check the value of the remote setting")
	}
	if strings.HasPrefix(opt.Remote, name+":") {
		return nil, errors.New("can't point imagewrap remote at itself - check the value of the remote setting")
	}

	if opt.BasePng != "embedded" {
		return nil, errors.New("imagewrap currently only supports base_png = embedded")
	}

	basePngBytes, err := base64.StdEncoding.DecodeString(basePngBase64)
	if err != nil {
		return nil, err
	}

	// Create the wrapped Fs
	baseFs, err := cache.Get(ctx, fspath.JoinRootPath(opt.Remote, root))
	if err != nil {
		return nil, err
	}

	f := &Fs{
		name:    name,
		root:    root,
		opt:     *opt,
		base:    baseFs,
		basePng: basePngBytes,
	}

	// Create Features
	f.features = (&fs.Features{
		CaseInsensitive:         baseFs.Features().CaseInsensitive,
		DuplicateFiles:          baseFs.Features().DuplicateFiles,
		ReadMimeType:            false,
		WriteMimeType:           false,
		CanHaveEmptyDirectories: baseFs.Features().CanHaveEmptyDirectories,
		BucketBased:             baseFs.Features().BucketBased,
		BucketBasedRootOK:       baseFs.Features().BucketBasedRootOK,
		SetTier:                 baseFs.Features().SetTier,
		GetTier:                 baseFs.Features().GetTier,
		SlowModTime:             baseFs.Features().SlowModTime,
		SlowHash:                true,
	}).Fill(ctx, f)

	return f, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS
func (f *Fs) String() string {
	return "ImageWrap '" + f.name + ":" + f.root + "'"
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return f.base.Precision()
}

// Hashes returns the supported hash types of the filesystem
func (f *Fs) Hashes() hash.Set {
	// We don't support hashes because we wrap the data
	return hash.Set(hash.None)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// List the objects and directories in dir into entries.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	entries, err = f.base.List(ctx, dir)
	if err != nil {
		return nil, err
	}

	var wrappedEntries fs.DirEntries
	for _, entry := range entries {
		switch x := entry.(type) {
		case fs.Directory:
			wrappedEntries = append(wrappedEntries, fs.NewDir(x.Remote(), x.ModTime(ctx)))
		case fs.Object:
			if strings.HasSuffix(x.Remote(), ".png") {
				wrappedEntries = append(wrappedEntries, f.wrapObject(x))
			} else {
				// According to detection logic: "If not, treat the file as a normal PNG and return it unchanged."
				// Wait, the prompt says:
				// "If not, treat the file as a normal PNG and return it unchanged."
				// But we are returning it unmodified anyway.
				wrappedEntries = append(wrappedEntries, x)
			}
		default:
			wrappedEntries = append(wrappedEntries, entry)
		}
	}
	return wrappedEntries, nil
}

// wrapObject wraps an underlying object into an imagewrap Object
func (f *Fs) wrapObject(o fs.Object) fs.Object {
	return &Object{
		fs:   f,
		base: o,
	}
}

// NewObject finds the Object at remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	// Let's first try appending .png
	o, err := f.base.NewObject(ctx, remote+".png")
	if err == nil {
		return f.wrapObject(o), nil
	}
	if err == fs.ErrorIsDir {
		return nil, err
	}

	// Maybe it's not a wrapped object
	o, err = f.base.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(remote, ".png") {
		// Just a regular file that happened to end in .png but wasn't stripped?
		return o, nil
	}

	// If the file exists and we were asking for `remote` (without .png) and it exists, maybe return it as is?
	// The problem statement says "treat the file as a normal PNG and return it unchanged." if it's a PNG but not wrapped.
	// But if someone asks for a wrapped object that doesn't exist, we should return the regular one.
	return o, nil
}

// Put in to the remote path with the modTime given of the given size
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Let's create an object that wraps the reader
	// But first, we need to create the wrapper
	wrappedReader, size := f.wrapReader(in, src.Remote(), src.Size())

	// Create a new ObjectInfo for the wrapped reader
	// The remote name gets .png appended
	wrappedRemote := src.Remote() + ".png"
	wrappedSrc := object.NewStaticObjectInfo(
		wrappedRemote,
		src.ModTime(ctx),
		size,
		true,
		nil,
		f.base,
	)

	o, err := f.base.Put(ctx, wrappedReader, wrappedSrc, options...)
	if err != nil {
		return nil, err
	}
	return f.wrapObject(o), nil
}

// Mkdir makes the directory
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return f.base.Mkdir(ctx, dir)
}

// Rmdir removes the directory
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return f.base.Rmdir(ctx, dir)
}

// wrapReader wraps the input io.Reader with the metadata and base PNG.
func (f *Fs) wrapReader(in io.Reader, filename string, originalSize int64) (io.Reader, int64) {
	// [base_png_bytes]
	// [magic_header]
	// [filename_length]
	// [filename]
	// [file_size]
	// [file_data]

	var meta bytes.Buffer
	// Add magic header
	meta.WriteString(magicHeader)
	// Add filename length
	filenameBytes := []byte(filename)
	binary.Write(&meta, binary.LittleEndian, uint16(len(filenameBytes)))
	// Add filename
	meta.Write(filenameBytes)
	// Add file size
	binary.Write(&meta, binary.LittleEndian, uint64(originalSize))

	header := io.MultiReader(bytes.NewReader(f.basePng), bytes.NewReader(meta.Bytes()))

	totalSize := int64(len(f.basePng)) + int64(meta.Len()) + originalSize
	if originalSize == -1 {
		totalSize = -1
	}

	return io.MultiReader(header, in), totalSize
}

// Purge all files in the directory
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context, dir string) error {
	doPurge := f.base.Features().Purge
	if doPurge == nil {
		return fs.ErrorCantPurge
	}
	return doPurge(ctx, dir)
}

// Copy src to this remote using server-side copy operations.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	// The prompt explicitly states: "Do NOT implement server-side Copy or Move."
	return nil, fs.ErrorCantCopy
}

// Move src to this remote using server-side move operations.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	// The prompt explicitly states: "Do NOT implement server-side Copy or Move."
	return nil, fs.ErrorCantMove
}

// DirMove moves src, srcRemote to this remote at dstRemote
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	// The prompt explicitly states: "Do NOT implement server-side Copy or Move."
	return fs.ErrorCantDirMove
}

// Object describes a wrapped file
type Object struct {
	fs   *Fs
	base fs.Object
}

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.fs
}

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.Remote()
}

// Remote returns the remote path
func (o *Object) Remote() string {
	remote := o.base.Remote()
	if strings.HasSuffix(remote, ".png") {
		return remote[:len(remote)-4]
	}
	return remote
}

// Hash returns the selected checksum of the file
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	return o.base.Storable()
}

// ModTime returns the modification date of the file
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.base.ModTime(ctx)
}

// SetModTime sets the metadata on the object to set the modification date
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return o.base.SetModTime(ctx, t)
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	// Calculate original size based on wrapped size
	// Wrapped Size = BasePng (len) + Magic (8) + NameLen (2) + Name (len) + Size (8) + Original Size
	// Therefore: Original Size = Wrapped Size - BasePng - 18 - NameLen
	remoteName := o.Remote()
	headerSize := int64(len(o.fs.basePng)) + 8 + 2 + int64(len(remoteName)) + 8
	wrappedSize := o.base.Size()
	if wrappedSize < headerSize {
		// It's smaller than a header, probably a normal PNG, return its actual size
		return wrappedSize
	}
	// For wrapped files, this will return the exact original size perfectly!
	return wrappedSize - headerSize
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// Calculate header size
	remoteName := o.Remote()
	headerSize := int64(len(o.fs.basePng)) + 8 + 2 + int64(len(remoteName)) + 8

	// Modify options for chunked/range downloads
	var newOptions []fs.OpenOption
	var isOffset bool
	for _, opt := range options {
		switch x := opt.(type) {
		case *fs.RangeOption:
			// Adjust range to skip header
			newOpt := &fs.RangeOption{Start: x.Start + headerSize, End: x.End}
			if x.End >= 0 {
				newOpt.End = x.End + headerSize
			}
			newOptions = append(newOptions, newOpt)
			if x.Start > 0 {
				isOffset = true
			}
		case *fs.SeekOption:
			// Adjust seek to skip header
			newOpt := &fs.SeekOption{Offset: x.Offset + headerSize}
			newOptions = append(newOptions, newOpt)
			if x.Offset > 0 {
				isOffset = true
			}
		case *fs.HTTPOption:
			// pass through
			newOptions = append(newOptions, x)
		default:
			newOptions = append(newOptions, x)
		}
	}

	// First open the underlying object
	rc, err := o.base.Open(ctx, newOptions...)
	if err != nil {
		return nil, err
	}

	// If the file was opened with an offset > 0, we can't verify the header because we are in the middle of the file.
	// Rclone expects exactly the requested bytes, and the underlying remote has already skipped the header + offset.
	// So we just return the stream.
	if isOffset {
		return rc, nil
	}

	// No offset requested, read and verify header as normal
	// Read the base PNG
	basePngBuf := make([]byte, len(o.fs.basePng))
	n, err := io.ReadFull(rc, basePngBuf)
	if err != nil {
		// If we can't even read the base PNG length, just return the file as is
		return newReadCloser(rc, rc, basePngBuf[:n]), nil
	}

	// Verify the base PNG matches (optional, but good for validation)
	// Actually, detection logic says:
	// A file should be treated as wrapped if:
	// 1. Extension is .png (we check this during listing/NewObject)
	// 2. After the base PNG bytes there is the magic header "RCIMG001"
	// So we need to read the magic header next.

	magicBuf := make([]byte, len(magicHeader))
	n, err = io.ReadFull(rc, magicBuf)
	if err != nil {
		return newReadCloser(rc, rc, append(basePngBuf, magicBuf[:n]...)), nil
	}

	if string(magicBuf) != magicHeader {
		// Not a wrapped file, return everything we read so far plus the rest of the stream
		return newReadCloser(rc, rc, append(basePngBuf, magicBuf...)), nil
	}

	// It's a wrapped file!
	// Now we need to parse the rest of the metadata
	// [filename_length] uint16
	var nameLen uint16
	err = binary.Read(rc, binary.LittleEndian, &nameLen)
	if err != nil {
		return nil, err
	}

	// [filename] bytes
	nameBuf := make([]byte, nameLen)
	_, err = io.ReadFull(rc, nameBuf)
	if err != nil {
		return nil, err
	}

	// [file_size] uint64
	var originalSize uint64
	err = binary.Read(rc, binary.LittleEndian, &originalSize)
	if err != nil {
		return nil, err
	}

	// The rest of the file is the file data.
	// rc is now pointing at the first byte of file_data.
	return rc, nil
}

// newReadCloser returns an io.ReadCloser that prepends buffered data before reading from rc
func newReadCloser(closer io.Closer, reader io.Reader, buf []byte) io.ReadCloser {
	multi := io.MultiReader(bytes.NewReader(buf), reader)
	return &multiReadCloser{
		Reader: multi,
		Closer: closer,
	}
}

type multiReadCloser struct {
	io.Reader
	io.Closer
}

// Update in to the object with the modTime given of the given size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	wrappedReader, size := o.fs.wrapReader(in, src.Remote(), src.Size())

	wrappedRemote := src.Remote() + ".png"
	wrappedSrc := object.NewStaticObjectInfo(
		wrappedRemote,
		src.ModTime(ctx),
		size,
		true,
		nil,
		o.fs.base,
	)

	return o.base.Update(ctx, wrappedReader, wrappedSrc, options...)
}

// Remove this object
func (o *Object) Remove(ctx context.Context) error {
	return o.base.Remove(ctx)
}

// newReadCloser func was already defined in previous block

