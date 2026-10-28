package partition

import "testing"

func TestShard(t *testing.T) {
	// Fixed values: other clients (e.g. an SDK) must get the same shards.
	tests := []struct {
		key  string
		want int
	}{
		{"", 1},  // FNV-1a("") = 2166136261
		{"a", 0}, // FNV-1a("a") = 3826002220
	}
	for _, tt := range tests {
		if got := Shard([]byte(tt.key), 5); got != tt.want {
			t.Errorf("Shard(%q, 5) = %d, want %d", tt.key, got, tt.want)
		}
	}
	for _, key := range []string{"x", "device-42", "orders"} {
		if a, b := Shard([]byte(key), 7), Shard([]byte(key), 7); a != b || a < 0 || a >= 7 {
			t.Errorf("Shard(%q, 7) = %d, %d", key, a, b)
		}
	}
}
