package main

import "testing"

func TestNormalizeBitswapChunkKB(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "default for missing value", in: 0, want: 1024},
		{name: "keeps 256", in: 256, want: 256},
		{name: "keeps 512", in: 512, want: 512},
		{name: "keeps 1024", in: 1024, want: 1024},
		{name: "rounds to nearest allowed value", in: 700, want: 512},
		{name: "caps above max", in: 2048, want: 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := normalizeBitswapChunkKB(tt.in)
			if got != tt.want {
				t.Fatalf("normalizeBitswapChunkKB(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}
