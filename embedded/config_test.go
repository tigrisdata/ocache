package embedded

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	stor "github.com/tigrisdata/ocache/storage"
)

func TestBuildStorageConfig_TopLevelOnly(t *testing.T) {
	cfg := &Config{
		DiskPath:        "/tmp/cache",
		TTL:             2 * time.Hour,
		MaxDiskUsage:    1 << 30,
		InlineThreshold: 32 * 1024,
	}

	sc := cfg.buildStorageConfig()

	assert.Equal(t, "/tmp/cache", sc.DiskPath)
	assert.Equal(t, int((2 * time.Hour).Seconds()), sc.TTL)
	assert.Equal(t, int64(1<<30), sc.MaxDiskUsage)
	assert.Equal(t, 32*1024, sc.InlineThreshold)
	assert.Zero(t, sc.CompactionThreads, "unset advanced fields stay at zero so storage defaults apply")
}

func TestBuildStorageConfig_AdvancedFlowsThrough(t *testing.T) {
	cfg := &Config{
		DiskPath: "/tmp/cache",
		TTL:      time.Hour,
		Storage: &stor.StorageConfig{
			CompactionThreads:    4,
			SegmentSize:          256 << 20,
			FdCacheSize:          2048,
			MetadataCacheSize:    512 << 20,
			CleanupInterval:      30 * time.Second,
			DisableRecompaction:  true,
			RecompactionInterval: 10 * time.Minute,
		},
	}

	sc := cfg.buildStorageConfig()

	assert.Equal(t, 4, sc.CompactionThreads)
	assert.Equal(t, int64(256<<20), sc.SegmentSize)
	assert.Equal(t, 2048, sc.FdCacheSize)
	assert.Equal(t, int64(512<<20), sc.MetadataCacheSize)
	assert.Equal(t, 30*time.Second, sc.CleanupInterval)
	assert.True(t, sc.DisableRecompaction)
	assert.Equal(t, 10*time.Minute, sc.RecompactionInterval)
}

func TestBuildStorageConfig_TopLevelOverridesStorage(t *testing.T) {
	cfg := &Config{
		DiskPath:        "/tmp/top",
		TTL:             time.Hour,
		MaxDiskUsage:    1 << 30,
		InlineThreshold: 16 * 1024,
		Storage: &stor.StorageConfig{
			DiskPath:        "/tmp/storage",
			TTL:             999, // ignored; overlaid from top level
			MaxDiskUsage:    1 << 20,
			InlineThreshold: 128 * 1024,
			SegmentSize:     512 << 20,
		},
	}

	sc := cfg.buildStorageConfig()

	assert.Equal(t, "/tmp/top", sc.DiskPath, "top-level DiskPath wins")
	assert.Equal(t, int(time.Hour.Seconds()), sc.TTL, "top-level TTL wins")
	assert.Equal(t, int64(1<<30), sc.MaxDiskUsage, "top-level MaxDiskUsage wins when set")
	assert.Equal(t, 16*1024, sc.InlineThreshold, "top-level InlineThreshold wins when >0")
	assert.Equal(t, int64(512<<20), sc.SegmentSize, "unshadowed Storage fields pass through")
}

func TestBuildStorageConfig_UnsetTopLevelFallsThroughToStorage(t *testing.T) {
	cfg := &Config{
		DiskPath: "/tmp/cache",
		TTL:      time.Hour,
		Storage: &stor.StorageConfig{
			MaxDiskUsage:    1 << 20,
			InlineThreshold: 128 * 1024,
		},
	}

	sc := cfg.buildStorageConfig()

	assert.Equal(t, int64(1<<20), sc.MaxDiskUsage, "Storage wins when top-level MaxDiskUsage is zero")
	assert.Equal(t, 128*1024, sc.InlineThreshold, "Storage wins when top-level InlineThreshold is zero")
}

func TestConfig_Validate(t *testing.T) {
	err := (&Config{TTL: time.Hour}).Validate()
	assert.ErrorContains(t, err, "DiskPath")

	err = (&Config{DiskPath: "/tmp"}).Validate()
	assert.ErrorContains(t, err, "TTL")

	err = (&Config{DiskPath: "/tmp", TTL: time.Hour}).Validate()
	assert.NoError(t, err)
}
