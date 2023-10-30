package diskqueue

import (
	"bufio"
	"bytes"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func Equal(t *testing.T, expected, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\t   %#v (expected)\n\n\t!= %#v (actual)\033[39m\n\n",
			filepath.Base(file), line, expected, actual)
		t.FailNow()
	}
}

func NotEqual(t *testing.T, expected, actual interface{}) {
	if reflect.DeepEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tnexp: %#v\n\n\tgot:  %#v\033[39m\n\n",
			filepath.Base(file), line, expected, actual)
		t.FailNow()
	}
}

func Nil(t *testing.T, object interface{}) {
	if !isNil(object) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\t   <nil> (expected)\n\n\t!= %#v (actual)\033[39m\n\n",
			filepath.Base(file), line, object)
		t.FailNow()
	}
}

func NotNil(t *testing.T, object interface{}) {
	if isNil(object) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tExpected value not to be <nil>\033[39m\n\n",
			filepath.Base(file), line)
		t.FailNow()
	}
}

func isNil(object interface{}) bool {
	if object == nil {
		return true
	}

	value := reflect.ValueOf(object)
	kind := value.Kind()
	if kind >= reflect.Chan && kind <= reflect.Slice && value.IsNil() {
		return true
	}

	return false
}

type tbLog interface {
	Log(...interface{})
}

func NewTestLogger(tbl tbLog) AppLogFunc {
	return func(lvl LogLevel, f string, args ...interface{}) {
		tbl.Log(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
}

func TestDiskQueue(t *testing.T) {
	l := NewTestLogger(t)

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := New(dqName, tmpDir, 1024, 4, 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)
	Equal(t, int64(0), dq.Depth())

	msg := []byte("test")
	err = dq.Put(msg)
	Nil(t, err)
	Equal(t, int64(1), dq.Depth())

	msgOut := <-dq.ReadChan()
	Equal(t, msg, msgOut)
}

func TestDiskQueueRoll(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_roll" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	ml := int64(len(msg))
	dq := New(dqName, tmpDir, 10*(ml+4), int32(ml), 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)
	Equal(t, int64(0), dq.Depth())

	for i := 0; i < 11; i++ {
		err := dq.Put(msg)
		Nil(t, err)
		Equal(t, int64(i+1), dq.Depth())
	}

	Equal(t, int64(1), dq.(*diskQueue).writeFileNum)
	Equal(t, int64(ml+4), dq.(*diskQueue).writePos)

	for i := 11; i > 0; i-- {
		Equal(t, msg, <-dq.ReadChan())
		Equal(t, int64(i-1), dq.Depth())
	}
}

func TestDiskQueuePeek(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_peek" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	ml := int64(len(msg))
	dq := New(dqName, tmpDir, 10*(ml+4), int32(ml), 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)
	Equal(t, int64(0), dq.Depth())

	t.Run("roll", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := dq.Put(msg)
			Nil(t, err)
			Equal(t, int64(i+1), dq.Depth())
		}

		for i := 10; i > 0; i-- {
			Equal(t, msg, <-dq.PeekChan())
			Equal(t, int64(i), dq.Depth())

			Equal(t, msg, <-dq.ReadChan())
			Equal(t, int64(i-1), dq.Depth())
		}

		Nil(t, dq.Empty())
	})

	t.Run("peek-read", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := dq.Put(msg)
			Nil(t, err)
			Equal(t, int64(i+1), dq.Depth())
		}

		for i := 10; i > 0; i-- {
			Equal(t, msg, <-dq.PeekChan())
			Equal(t, int64(i), dq.Depth())

			Equal(t, msg, <-dq.PeekChan())
			Equal(t, int64(i), dq.Depth())

			Equal(t, msg, <-dq.ReadChan())
			Equal(t, int64(i-1), dq.Depth())
		}

		Nil(t, dq.Empty())
	})

	t.Run("read-peek", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := dq.Put(msg)
			Nil(t, err)
			Equal(t, int64(i+1), dq.Depth())
		}

		for i := 10; i > 1; i-- {
			Equal(t, msg, <-dq.PeekChan())
			Equal(t, int64(i), dq.Depth())

			Equal(t, msg, <-dq.ReadChan())
			Equal(t, int64(i-1), dq.Depth())

			Equal(t, msg, <-dq.PeekChan())
			Equal(t, int64(i-1), dq.Depth())
		}

		Nil(t, dq.Empty())
	})

}

func assertFileNotExist(t *testing.T, fn string) {
	f, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	Equal(t, (*os.File)(nil), f)
	Equal(t, true, os.IsNotExist(err))
}

func TestDiskQueueEmpty(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_empty" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	dq := New(dqName, tmpDir, 100, 0, 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)
	Equal(t, int64(0), dq.Depth())

	for i := 0; i < 100; i++ {
		err := dq.Put(msg)
		Nil(t, err)
		Equal(t, int64(i+1), dq.Depth())
	}

	for i := 0; i < 3; i++ {
		<-dq.ReadChan()
	}

	for {
		if dq.Depth() == 97 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	Equal(t, int64(97), dq.Depth())

	numFiles := dq.(*diskQueue).writeFileNum
	dq.Empty()

	assertFileNotExist(t, dq.(*diskQueue).metaDataFileName())
	for i := int64(0); i <= numFiles; i++ {
		assertFileNotExist(t, dq.(*diskQueue).fileName(i))
	}
	Equal(t, int64(0), dq.Depth())
	Equal(t, dq.(*diskQueue).writeFileNum, dq.(*diskQueue).readFileNum)
	Equal(t, dq.(*diskQueue).writePos, dq.(*diskQueue).readPos)
	Equal(t, dq.(*diskQueue).readPos, dq.(*diskQueue).nextReadPos)
	Equal(t, dq.(*diskQueue).readFileNum, dq.(*diskQueue).nextReadFileNum)

	for i := 0; i < 100; i++ {
		err := dq.Put(msg)
		Nil(t, err)
		Equal(t, int64(i+1), dq.Depth())
	}

	for i := 0; i < 100; i++ {
		<-dq.ReadChan()
	}

	for {
		if dq.Depth() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	Equal(t, int64(0), dq.Depth())
	Equal(t, dq.(*diskQueue).writeFileNum, dq.(*diskQueue).readFileNum)
	Equal(t, dq.(*diskQueue).writePos, dq.(*diskQueue).readPos)
	Equal(t, dq.(*diskQueue).readPos, dq.(*diskQueue).nextReadPos)
}

func TestDiskQueueCorruption(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_corruption" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	// require a non-zero message length for the corrupt (len 0) test below
	dq := New(dqName, tmpDir, 1000, 10, 1<<10, 5, 2*time.Second, l)
	defer dq.Close()

	msg := make([]byte, 120) // 124 bytes per message, 8 messages (992 bytes) per file
	msg[0] = 91
	msg[62] = 4
	msg[119] = 211

	for i := 0; i < 25; i++ {
		dq.Put(msg)
	}

	Equal(t, int64(25), dq.Depth())

	// corrupt the 2nd file
	dqFn := dq.(*diskQueue).fileName(1)
	os.Truncate(dqFn, 400) // 3 valid messages, 5 corrupted

	// corrupt the 4th (current) file
	dqFn = dq.(*diskQueue).fileName(3)
	os.Truncate(dqFn, 100)

	for i := 0; i < 18; i++ {
		Equal(t, msg, <-dq.ReadChan())
	}
	Equal(t, int64(7), dq.Depth())

	Equal(t, msg, <-dq.ReadChan())
	Equal(t, int64(0), dq.Depth())

	dq.Put(msg) // in 5th file

	Equal(t, msg, <-dq.ReadChan())
	Equal(t, int64(0), dq.Depth())

	// write a corrupt (len 0) message at the 5th (current) file
	dq.(*diskQueue).writeFile.Write([]byte{0, 0, 0, 0})

	// force a new 6th file - put into 5th, then readOne errors, then put into 6th
	dq.Put(msg)
	dq.Put(msg)

	Equal(t, msg, <-dq.ReadChan())

	dq.Put(msg)
	dq.Put(msg)

	// corrupt the last file
	dqFn = dq.(*diskQueue).fileName(5)
	os.Truncate(dqFn, 100)

	Equal(t, int64(2), dq.Depth())

	// return one message and try reading again from corrupted file
	Equal(t, msg, <-dq.ReadChan())

	// give diskqueue time to handle read error
	time.Sleep(50 * time.Millisecond)

	// the last log file is now considered corrupted leaving no more log messages
	Equal(t, int64(0), dq.Depth())
}

type md struct {
	depth        int64
	readFileNum  int64
	writeFileNum int64
	readPos      int64
	writePos     int64
}

func readMetaDataFile(fileName string, retried int) md {
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		// provide a simple retry that results in up to
		// another 500ms for the file to be written.
		if retried < 9 {
			retried++
			time.Sleep(50 * time.Millisecond)
			return readMetaDataFile(fileName, retried)
		}
		panic(err)
	}
	defer f.Close()

	var ret md
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&ret.depth,
		&ret.readFileNum, &ret.readPos,
		&ret.writeFileNum, &ret.writePos)
	if err != nil {
		panic(err)
	}
	return ret
}

func TestDiskQueueSyncAfterRead(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_read_after_sync" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := New(dqName, tmpDir, 1<<11, 0, 1<<10, 2500, 50*time.Millisecond, l)
	defer dq.Close()

	msg := make([]byte, 1000)
	dq.Put(msg)

	for i := 0; i < 10; i++ {
		d := readMetaDataFile(dq.(*diskQueue).metaDataFileName(), 0)
		if d.depth == 1 &&
			d.readFileNum == 0 &&
			d.writeFileNum == 0 &&
			d.readPos == 0 &&
			d.writePos == 1004 {
			// success
			goto next
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("fail")

next:
	dq.Put(msg)
	<-dq.ReadChan()

	for i := 0; i < 10; i++ {
		d := readMetaDataFile(dq.(*diskQueue).metaDataFileName(), 0)
		if d.depth == 1 &&
			d.readFileNum == 0 &&
			d.writeFileNum == 0 &&
			d.readPos == 1004 &&
			d.writePos == 2008 {
			// success
			goto done
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("fail")

done:
}

func TestDiskQueueTorture(t *testing.T) {
	var wg sync.WaitGroup

	l := NewTestLogger(t)
	dqName := "test_disk_queue_torture" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := New(dqName, tmpDir, 262144, 0, 1<<10, 2500, 2*time.Second, l)
	NotNil(t, dq)
	Equal(t, int64(0), dq.Depth())

	msg := []byte("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")

	numWriters := 4
	numReaders := 4
	readExitChan := make(chan int)
	writeExitChan := make(chan int)

	var depth int64
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case <-writeExitChan:
					return
				default:
					err := dq.Put(msg)
					if err == nil {
						atomic.AddInt64(&depth, 1)
					}
				}
			}
		}()
	}

	time.Sleep(1 * time.Second)

	dq.Close()

	t.Logf("closing writeExitChan")
	close(writeExitChan)
	wg.Wait()

	t.Logf("restarting diskqueue")

	dq = New(dqName, tmpDir, 262144, 0, 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)
	Equal(t, depth, dq.Depth())

	var read int64
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case m := <-dq.ReadChan():
					Equal(t, m, msg)
					atomic.AddInt64(&read, 1)
				case <-readExitChan:
					return
				}
			}
		}()
	}

	t.Logf("waiting for depth 0")
	for {
		if dq.Depth() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("closing readExitChan")
	close(readExitChan)
	wg.Wait()

	Equal(t, depth, read)
}

func TestDiskQueueResize(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_resize" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	ml := int64(len(msg))
	dq := New(dqName, tmpDir, 8*(ml+4), int32(ml), 1<<10, 2500, time.Second, l)
	NotNil(t, dq)
	Equal(t, int64(0), dq.Depth())

	for i := 0; i < 9; i++ {
		msg[0] = byte(i)
		err := dq.Put(msg)
		Nil(t, err)
	}
	Equal(t, int64(1), dq.(*diskQueue).writeFileNum)
	Equal(t, int64(ml+4), dq.(*diskQueue).writePos)
	Equal(t, int64(9), dq.Depth())

	dq.Close()
	dq = New(dqName, tmpDir, 10*(ml+4), int32(ml), 1<<10, 2500, time.Second, l)

	for i := 0; i < 10; i++ {
		msg[0] = byte(20 + i)
		err := dq.Put(msg)
		Nil(t, err)
	}
	Equal(t, int64(2), dq.(*diskQueue).writeFileNum)
	Equal(t, int64(ml+4), dq.(*diskQueue).writePos)
	Equal(t, int64(19), dq.Depth())

	for i := 0; i < 9; i++ {
		msg[0] = byte(i)
		Equal(t, msg, <-dq.ReadChan())
	}
	for i := 0; i < 10; i++ {
		msg[0] = byte(20 + i)
		Equal(t, msg, <-dq.ReadChan())
	}
	Equal(t, int64(0), dq.Depth())
	dq.Close()

	// make sure there aren't "bad" files due to read logic errors
	files, err := filepath.Glob(filepath.Join(tmpDir, dqName+"*.bad"))
	Nil(t, err)
	// empty files slice is actually nil, length check is less confusing
	if len(files) > 0 {
		Equal(t, []string{}, files)
	}
}

func BenchmarkDiskQueuePut16(b *testing.B) {
	benchmarkDiskQueuePut(16, b)
}
func BenchmarkDiskQueuePut64(b *testing.B) {
	benchmarkDiskQueuePut(64, b)
}
func BenchmarkDiskQueuePut256(b *testing.B) {
	benchmarkDiskQueuePut(256, b)
}
func BenchmarkDiskQueuePut1024(b *testing.B) {
	benchmarkDiskQueuePut(1024, b)
}
func BenchmarkDiskQueuePut4096(b *testing.B) {
	benchmarkDiskQueuePut(4096, b)
}
func BenchmarkDiskQueuePut16384(b *testing.B) {
	benchmarkDiskQueuePut(16384, b)
}
func BenchmarkDiskQueuePut65536(b *testing.B) {
	benchmarkDiskQueuePut(65536, b)
}
func BenchmarkDiskQueuePut262144(b *testing.B) {
	benchmarkDiskQueuePut(262144, b)
}
func BenchmarkDiskQueuePut1048576(b *testing.B) {
	benchmarkDiskQueuePut(1048576, b)
}
func benchmarkDiskQueuePut(size int64, b *testing.B) {
	b.StopTimer()
	l := NewTestLogger(b)
	dqName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := New(dqName, tmpDir, 1024768*100, 0, 1<<20, 2500, 2*time.Second, l)
	defer dq.Close()
	b.SetBytes(size)
	data := make([]byte, size)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err := dq.Put(data)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkDiskWrite16(b *testing.B) {
	benchmarkDiskWrite(16, b)
}
func BenchmarkDiskWrite64(b *testing.B) {
	benchmarkDiskWrite(64, b)
}
func BenchmarkDiskWrite256(b *testing.B) {
	benchmarkDiskWrite(256, b)
}
func BenchmarkDiskWrite1024(b *testing.B) {
	benchmarkDiskWrite(1024, b)
}
func BenchmarkDiskWrite4096(b *testing.B) {
	benchmarkDiskWrite(4096, b)
}
func BenchmarkDiskWrite16384(b *testing.B) {
	benchmarkDiskWrite(16384, b)
}
func BenchmarkDiskWrite65536(b *testing.B) {
	benchmarkDiskWrite(65536, b)
}
func BenchmarkDiskWrite262144(b *testing.B) {
	benchmarkDiskWrite(262144, b)
}
func BenchmarkDiskWrite1048576(b *testing.B) {
	benchmarkDiskWrite(1048576, b)
}
func benchmarkDiskWrite(size int64, b *testing.B) {
	b.StopTimer()
	fileName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	f, _ := os.OpenFile(path.Join(tmpDir, fileName), os.O_RDWR|os.O_CREATE, 0600)
	b.SetBytes(size)
	data := make([]byte, size)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		f.Write(data)
	}
	f.Sync()
}

func BenchmarkDiskWriteBuffered16(b *testing.B) {
	benchmarkDiskWriteBuffered(16, b)
}
func BenchmarkDiskWriteBuffered64(b *testing.B) {
	benchmarkDiskWriteBuffered(64, b)
}
func BenchmarkDiskWriteBuffered256(b *testing.B) {
	benchmarkDiskWriteBuffered(256, b)
}
func BenchmarkDiskWriteBuffered1024(b *testing.B) {
	benchmarkDiskWriteBuffered(1024, b)
}
func BenchmarkDiskWriteBuffered4096(b *testing.B) {
	benchmarkDiskWriteBuffered(4096, b)
}
func BenchmarkDiskWriteBuffered16384(b *testing.B) {
	benchmarkDiskWriteBuffered(16384, b)
}
func BenchmarkDiskWriteBuffered65536(b *testing.B) {
	benchmarkDiskWriteBuffered(65536, b)
}
func BenchmarkDiskWriteBuffered262144(b *testing.B) {
	benchmarkDiskWriteBuffered(262144, b)
}
func BenchmarkDiskWriteBuffered1048576(b *testing.B) {
	benchmarkDiskWriteBuffered(1048576, b)
}
func benchmarkDiskWriteBuffered(size int64, b *testing.B) {
	b.StopTimer()
	fileName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	f, _ := os.OpenFile(path.Join(tmpDir, fileName), os.O_RDWR|os.O_CREATE, 0600)
	b.SetBytes(size)
	data := make([]byte, size)
	w := bufio.NewWriterSize(f, 1024*4)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		w.Write(data)
		if i%1024 == 0 {
			w.Flush()
		}
	}
	w.Flush()
	f.Sync()
}

// you might want to run this like
// $ go test -bench=DiskQueueGet -benchtime 0.1s
// too avoid doing too many iterations.
func BenchmarkDiskQueueGet16(b *testing.B) {
	benchmarkDiskQueueGet(16, b)
}
func BenchmarkDiskQueueGet64(b *testing.B) {
	benchmarkDiskQueueGet(64, b)
}
func BenchmarkDiskQueueGet256(b *testing.B) {
	benchmarkDiskQueueGet(256, b)
}
func BenchmarkDiskQueueGet1024(b *testing.B) {
	benchmarkDiskQueueGet(1024, b)
}
func BenchmarkDiskQueueGet4096(b *testing.B) {
	benchmarkDiskQueueGet(4096, b)
}
func BenchmarkDiskQueueGet16384(b *testing.B) {
	benchmarkDiskQueueGet(16384, b)
}
func BenchmarkDiskQueueGet65536(b *testing.B) {
	benchmarkDiskQueueGet(65536, b)
}
func BenchmarkDiskQueueGet262144(b *testing.B) {
	benchmarkDiskQueueGet(262144, b)
}
func BenchmarkDiskQueueGet1048576(b *testing.B) {
	benchmarkDiskQueueGet(1048576, b)
}

func benchmarkDiskQueueGet(size int64, b *testing.B) {
	b.StopTimer()
	l := NewTestLogger(b)
	dqName := "bench_disk_queue_get" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := New(dqName, tmpDir, 1024768, 0, 1<<30, 2500, 2*time.Second, l)
	defer dq.Close()
	b.SetBytes(size)
	data := make([]byte, size)
	for i := 0; i < b.N; i++ {
		dq.Put(data)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		<-dq.ReadChan()
	}
}

func TestDiskQueueRollAsync(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_roll" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	ml := int64(len(msg))
	dq := New(dqName, tmpDir, 10*(ml+4), int32(ml), 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)
	Equal(t, int64(0), dq.Depth())

	for i := 0; i < 11; i++ {
		err := dq.Put(msg)
		Nil(t, err)
		Equal(t, int64(1), dq.Depth())

		Equal(t, msg, <-dq.ReadChan())
		Equal(t, int64(0), dq.Depth())
	}

	Equal(t, int64(1), dq.(*diskQueue).writeFileNum)
	Equal(t, int64(ml+4), dq.(*diskQueue).writePos)

	filepath.Walk(tmpDir, func(path string, info fs.FileInfo, err error) error {
		if strings.HasSuffix(path, ".bad") {
			t.FailNow()
		}

		return err
	})
}

func TestWriteRollReadEOF(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_roll_readEOF" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := New(dqName, tmpDir, 1024, 4, 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)
	Equal(t, int64(0), dq.Depth())

	for i := 0; i < 205; i++ { // 204 messages fit, but message 205 will be too big
		msg := []byte(fmt.Sprintf("%05d", i)) // 5 bytes
		err = dq.Put(msg)

		msgOut := <-dq.ReadChan()
		Equal(t, msg, msgOut)
	}

	filepath.Walk(tmpDir, func(path string, info fs.FileInfo, err error) error {
		if strings.HasSuffix(path, ".bad") {
			t.FailNow()
		}

		return err
	})
}
