package columnstore

import (
	"sync"
	"testing"
)

// 需求：同一数据集的读取，所打开的文件数要有上界，不随小文件数量线性增长。
// 实现策略是逐段顺序打开、读完即关。本测试通过钩子统计 ReadColumns 过程中
// "同时"打开的段文件句柄峰值。
func TestReadOpenFilesBounded(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), &Options{MaxSegments: 100000}) // 关闭自动合并
	// 制造大量小文件（每个批次一个段）。
	const batches = 40
	for i := 0; i < batches; i++ {
		writeRows(t, s, i*2+1, 2)
	}
	if st := logVisible(t, s, "many small files"); st.SegmentCount != batches {
		t.Fatalf("setup: want %d segments, got %d", batches, st.SegmentCount)
	}

	var mu sync.Mutex
	open := 0
	peak := 0
	openHook = func(string) {
		mu.Lock()
		open++
		if open > peak {
			peak = open
		}
		mu.Unlock()
	}
	closeHook = func(string) {
		mu.Lock()
		open--
		mu.Unlock()
	}
	t.Cleanup(func() {
		openHook = nil
		closeHook = nil
	})

	rs, err := s.ReadColumns([]string{"id", "name", "score"})
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows != batches*2 {
		t.Fatalf("rows=%d want %d", rs.Rows, batches*2)
	}
	mu.Lock()
	gotPeak := peak
	gotOpen := open
	mu.Unlock()
	t.Logf("JUDGE: %d segment files on disk, but peak concurrently open during scan = %d, remaining open = %d",
		batches, gotPeak, gotOpen)
	// 无论磁盘上有多少段文件，单条扫描路径任意时刻最多打开 1 个段文件。
	if gotPeak != 1 {
		t.Fatalf("expected peak 1 concurrent open segment file, got %d", gotPeak)
	}
	if gotOpen != 0 {
		t.Fatalf("expected all segment files closed after scan, %d remain", gotOpen)
	}
}
