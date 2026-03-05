// Copyright (c) 2025 Cisco and/or its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// ring-buffer-reader is an example consumer for the SFDP session stats
// ring buffer exposed by VPP in the stats segment. It connects to the
// VPP stats socket, validates the ring buffer schema, and continuously
// polls for new entries, printing decoded session stats to stdout.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.fd.io/govpp/adapter"
	"go.fd.io/govpp/adapter/statsclient"
	"go.fd.io/govpp/binapi/session_stats"
)

const sfdpSessionStatsRing = "/sfdp/session/stats"

// expectedAbiID is the schema ABI identifier that this consumer supports.
// It is constructed from the message name and CRC of the generated
// SfdpSessionStatsRingEntryAbiID message.
var expectedAbiID = (&session_stats.SfdpSessionStatsRingEntryAbiID{}).GetMessageName() +
	"_" + (&session_stats.SfdpSessionStatsRingEntryAbiID{}).GetCrcString()

var (
	statsSocket = flag.String("socket", statsclient.DefaultSocketName, "Path to VPP stats socket")
	pollPeriod  = flag.Duration("period", time.Second, "Poll interval")
	debug       = flag.Bool("debug", false, "Enable debug output")
)

// threadConsumerState tracks the per-thread read position in the ring buffer.
type threadConsumerState struct {
	localTail    uint32
	lastSequence uint64
	initialized  bool
}

func main() {
	flag.Parse()

	client := statsclient.NewStatsClient(*statsSocket)
	if err := client.Connect(); err != nil {
		log.Fatalf("connecting to stats socket: %v", err)
	}

	// golint - Discard disconnect result to address linter issue
	defer func() { _ = client.Disconnect() }()

	// Prepare a stat dir containing only the SFDP session stats ring buffer entry.
	dir, err := client.PrepareDir(sfdpSessionStatsRing)
	if err != nil {
		log.Fatalf("preparing dir: %v", err)
	}

	rb := findRingBuffer(dir)
	if rb == nil {
		log.Fatalf("ring buffer entry %q not found", sfdpSessionStatsRing)
	}

	// Validate schema string, by checking that ABI ID string set in schema is valid
	if err := validateSchema(rb); err != nil {
		log.Fatalf("schema validation failed: %v", err)
	}

	// Initialize per-thread consumer state: start at head, skip old data.
	states := initConsumerStates(rb)

	fmt.Printf("Connected to ring buffer %s\n", sfdpSessionStatsRing)
	fmt.Printf("  entry_size=%d ring_size=%d threads=%d schema_version=%d\n",
		rb.Config.EntrySize, rb.Config.RingSize, rb.Config.NThreads, rb.Config.SchemaVersion)
	if len(rb.Schema) > 0 {
		s := rb.Schema
		// Account for null-char
		if s[len(s)-1] == 0 {
			s = s[:len(s)-1]
		}
		fmt.Printf("  schema: %s\n", string(s))
	}
	fmt.Printf("Polling every %v (Ctrl-C to stop)\n\n", *pollPeriod)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(*pollPeriod)
	defer ticker.Stop()

	var totalConsumed, totalMissed uint64
	for {
		select {
		case <-sigCh:
			fmt.Printf("\nTotal consumed: %d, total missed: %d\n", totalConsumed, totalMissed)
			return
		case <-ticker.C:
		}

		// Update the stat dir to get a fresh snapshot.
		if err := client.UpdateDir(dir); err != nil {
			if err == adapter.ErrStatsDirStale {
				dir, err = client.PrepareDir(sfdpSessionStatsRing)
				if err != nil {
					log.Fatalf("re-preparing dir: %v", err)
				}
				rb = findRingBuffer(dir)
				if rb == nil {
					log.Printf("ring buffer does not exist anymore, retrying...")
					continue
				}
				// Re-initialize consumer state on epoch change.
				states = initConsumerStates(rb)
				continue
			}
			log.Printf("updating dir: %v", err)
			continue
		}

		rb = findRingBuffer(dir)
		if rb == nil {
			continue
		}

		// Consume & display ring buffer entries
		// and account for potential misses (if VPP produces
		// faster than we can consume)
		consumed, missed := consumeEntries(rb, states)
		totalConsumed += consumed
		totalMissed += missed
	}
}

func initConsumerStates(rb *adapter.RingBufferStat) []threadConsumerState {
	states := make([]threadConsumerState, rb.Config.NThreads)
	for i, t := range rb.Threads {
		states[i] = threadConsumerState{
			localTail:    t.Head,
			lastSequence: t.Sequence,
			initialized:  true,
		}
	}
	return states
}

// findRingBuffer returns the RingBufferStat from the dir, or nil.
func findRingBuffer(dir *adapter.StatDir) *adapter.RingBufferStat {
	for i := range dir.Entries {
		if rb, ok := dir.Entries[i].Data.(adapter.RingBufferStat); ok {
			return &rb
		}
	}
	return nil
}

// validateSchema checks the schema is present and matches the expected ABI ID
// from the generated session_stats bindings, similar to the C consumer's
// ensure_schema_loaded.
func validateSchema(rb *adapter.RingBufferStat) error {
	if len(rb.Threads) == 0 {
		return fmt.Errorf("no threads in ring buffer")
	}

	if rb.Threads[0].SchemaSize == 0 {
		return fmt.Errorf("no schema found in ring metadata")
	}

	if len(rb.Schema) == 0 {
		return fmt.Errorf("schema bytes not available")
	}

	// Verify that the schema byte length matches the size declared in
	// the per-thread metadata and the ring buffer config header.
	if uint32(len(rb.Schema)) != rb.Threads[0].SchemaSize {
		return fmt.Errorf("schema length mismatch: got %d bytes, thread metadata declares %d",
			len(rb.Schema), rb.Threads[0].SchemaSize)
	}
	if rb.Config.SchemaSize != 0 && uint32(len(rb.Schema)) != rb.Config.SchemaSize {
		return fmt.Errorf("schema length mismatch: got %d bytes, config header declares %d",
			len(rb.Schema), rb.Config.SchemaSize)
	}

	// Strip the trailing null if present, then compare as string.
	schemaBytes := rb.Schema
	if schemaBytes[len(schemaBytes)-1] == 0 {
		schemaBytes = schemaBytes[:len(schemaBytes)-1]
	}
	schemaStr := string(schemaBytes)
	fmt.Printf("  schema ABI ID: %s\n", schemaStr)

	if schemaStr != expectedAbiID {
		return fmt.Errorf("schema ABI ID mismatch: ring has %q, expected %q",
			schemaStr, expectedAbiID)
	}

	return nil
}

// consumeEntries reads new entries from the ring buffer and prints them.
// Returns the number of entries consumed and missed.
func consumeEntries(rb *adapter.RingBufferStat, states []threadConsumerState) (consumed, missed uint64) {
	// Iterate over thread data stored in the ring buffer
	for threadIdx := uint32(0); threadIdx < rb.Config.NThreads; threadIdx++ {
		meta := rb.Threads[threadIdx]
		state := &states[threadIdx]

		if !state.initialized {
			state.localTail = meta.Head
			state.lastSequence = meta.Sequence
			state.initialized = true
			continue
		}

		delta := meta.Sequence - state.lastSequence

		if *debug {
			fmt.Fprintf(os.Stderr, "DBG: thread %d: seq=%d last_seq=%d delta=%d head=%d tail=%d\n",
				threadIdx, meta.Sequence, state.lastSequence, delta, meta.Head, state.localTail)
		}

		if delta == 0 {
			continue
		}

		threadData := rb.Data[threadIdx]

		// Overflow: delta >= ring_size means we lost entries.
		if delta >= uint64(rb.Config.RingSize) {
			overflow := delta - uint64(rb.Config.RingSize)
			if overflow > 0 {
				missed += overflow
				fmt.Fprintf(os.Stderr, "WARNING: thread %d missed %d entries (delta=%d, ring_size=%d)\n",
					threadIdx, overflow, delta, rb.Config.RingSize)
			}

			// Start consuming from head (oldest surviving entry).
			state.localTail = meta.Head
			for i := uint32(0); i < rb.Config.RingSize; i++ {
				entry := decodeEntry(threadData, state.localTail, rb.Config.EntrySize)
				// Ignore invalid entries
				if entry != nil && entry.SessionID != 0 {
					printEntry(threadIdx, entry)
					consumed++
				}
				state.localTail = (state.localTail + 1) % rb.Config.RingSize
			}
			state.lastSequence = meta.Sequence
			continue
		}

		// Normal case: consume from tail to head.
		for state.localTail != meta.Head {
			entry := decodeEntry(threadData, state.localTail, rb.Config.EntrySize)
			// Ignore invalid entries
			if entry != nil && entry.SessionID != 0 {
				printEntry(threadIdx, entry)
				consumed++
			}
			state.localTail = (state.localTail + 1) % rb.Config.RingSize
		}
		state.lastSequence = meta.Sequence
	}
	return
}

// decodeEntry decodes a single ring entry from thread data
// which takes the format of 'SfdpSessionStatsRingEntry' generated from VPP API
func decodeEntry(threadData []byte, slot, entrySize uint32) *session_stats.SfdpSessionStatsRingEntry {
	offset := uint64(slot) * uint64(entrySize)
	end := offset + uint64(entrySize)
	if end > uint64(len(threadData)) {
		return nil
	}

	// Unmarshal entry, and decode it from VPP API wire format (big-endian) to host order.
	var msg session_stats.SfdpSessionStatsRingEntryAbiID
	if err := msg.Unmarshal(threadData[offset:end]); err != nil {
		if *debug {
			fmt.Fprintf(os.Stderr, "DBG: failed to decode entry at slot %d: %v\n", slot, err)
		}
		return nil
	}
	return &msg.Entry
}

// printEntry prints a decoded session stats entry to stdout.
func printEntry(threadIdx uint32, e *session_stats.SfdpSessionStatsRingEntry) {
	src := formatIP(e.SrcIP, e.IsIP4)
	dst := formatIP(e.DstIP, e.IsIP4)
	ipVer := "ip6"
	if e.IsIP4 != 0 {
		ipVer = "ip4"
	}

	fmt.Printf("thread %d: session_id=%d tenant_id=%d %s %s:%d -> %s:%d proto=%s type=%d\n",
		threadIdx, e.SessionID, e.TenantID, ipVer, src, e.SrcPort, dst, e.DstPort, formatProto(e.Proto), e.SessionType)
	fmt.Printf("  packets: fwd=%d rev=%d, bytes: fwd=%d rev=%d, duration=%.3fs\n",
		e.PacketsForward, e.PacketsReverse, e.BytesForward, e.BytesReverse, e.Duration)
	fmt.Printf("  ttl fwd: min=%d max=%d mean=%.1f stddev=%.1f, rev: min=%d max=%d mean=%.1f stddev=%.1f\n",
		e.TTLMinForward, e.TTLMaxForward, e.TTLMeanForward, e.TTLStddevForward,
		e.TTLMinReverse, e.TTLMaxReverse, e.TTLMeanReverse, e.TTLStddevReverse)
	fmt.Printf("  rtt fwd: mean=%.3fms stddev=%.3fms, rev: mean=%.3fms stddev=%.3fms\n",
		e.RttMeanForward*1000, e.RttStddevForward*1000,
		e.RttMeanReverse*1000, e.RttStddevReverse*1000)
	fmt.Printf("  tcp: syn=%d fin=%d rst=%d mss=%d handshake=%v\n",
		e.TCPSynPackets, e.TCPFinPackets, e.TCPRstPackets, e.TCPMss, e.TCPHandshakeComplete != 0)
	fmt.Printf("  tcp: retrans fwd=%d rev=%d, dupack fwd=%d rev=%d, zero_win fwd=%d rev=%d\n",
		e.TCPRetransmissionsFwd, e.TCPRetransmissionsRev,
		e.TCPDupackEventsFwd, e.TCPDupackEventsRev,
		e.TCPZeroWindowEventsFwd, e.TCPZeroWindowEventsRev)
	fmt.Printf("  opaque=%d\n", e.Opaque)
}

// formatProto returns string for main ip proto type.
func formatProto(proto uint8) string {
	switch proto {
	case 1:
		return "ICMP"
	case 2:
		return "IGMP"
	case 6:
		return "TCP"
	case 17:
		return "UDP"
	case 47:
		return "GRE"
	case 50:
		return "ESP"
	case 51:
		return "AH"
	case 58:
		return "ICMPv6"
	case 132:
		return "SCTP"
	default:
		return fmt.Sprintf("%d", proto)
	}
}

// formatIP formats an IP address from the 16-byte wire representation.
func formatIP(ip []byte, isIP4 uint8) string {
	if isIP4 != 0 {
		if len(ip) >= 4 {
			return net.IPv4(ip[0], ip[1], ip[2], ip[3]).String()
		}
		return "<invalid>"
	}
	if len(ip) >= 16 {
		return net.IP(ip[:16]).String()
	}
	return "<invalid>"
}
