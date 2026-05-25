package arrowserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/markfriz/wb-ozon-review-collector/internal/models"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ==========================================================================
// Схема
// ==========================================================================

func WindowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "product_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{
			Name:     "window_start",
			Type:     &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
			Nullable: false,
		},
		{Name: "avg_rating", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "total_likes", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "review_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
}

// ==========================================================================
// Ticket — структура для кодирования запроса клиента
// ==========================================================================

// QueryTicket кодирует фильтр, передаваемый в Flight.Ticket.
type QueryTicket struct {
	Cmd       string `json:"cmd"`                 // "all" | "filter"
	ProductID string `json:"product_id,omitempty"` // для filter
}

// EncodeTicket сериализует QueryTicket в JSON.
func EncodeTicket(cmd, productID string) []byte {
	t := QueryTicket{Cmd: cmd, ProductID: productID}
	data, _ := json.Marshal(t)
	return data
}

// ParseTicket десериализует Ticket в QueryTicket.
func ParseTicket(raw []byte) (QueryTicket, error) {
	var qt QueryTicket
	if err := json.Unmarshal(raw, &qt); err != nil {
		return qt, fmt.Errorf("parse ticket: %w", err)
	}
	return qt, nil
}

// ==========================================================================
// WindowStore
// ==========================================================================

type WindowStore struct {
	mu   sync.RWMutex
	rows []models.WindowAgg
	max  int
}

func NewWindowStore(maxRecords int) *WindowStore {
	return &WindowStore{
		rows: make([]models.WindowAgg, 0, 1000),
		max:  maxRecords,
	}
}

func (s *WindowStore) Push(agg models.WindowAgg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, agg)
	if s.max > 0 && len(s.rows) > s.max {
		s.rows = s.rows[len(s.rows)-s.max:]
	}
}

func (s *WindowStore) Snapshot() []models.WindowAgg {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make([]models.WindowAgg, len(s.rows))
	copy(cp, s.rows)
	return cp
}

// SnapshotFiltered возвращает только записи, удовлетворяющие productID.
// Если productID пуст — возвращает всё.
func (s *WindowStore) SnapshotFiltered(productID string) []models.WindowAgg {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if productID == "" {
		cp := make([]models.WindowAgg, len(s.rows))
		copy(cp, s.rows)
		return cp
	}

	var filtered []models.WindowAgg
	for _, r := range s.rows {
		if r.ProductID == productID || strings.HasPrefix(r.ProductID, productID) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func (s *WindowStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

// ==========================================================================
// FlightServer
// ==========================================================================

type FlightServer struct {
	flight.BaseFlightServer
	store *WindowStore
	log   *zap.Logger
	alloc memory.Allocator
}

func NewFlightServer(store *WindowStore, logger *zap.Logger) *FlightServer {
	return &FlightServer{
		store: store,
		log:   logger,
		alloc: memory.NewGoAllocator(),
	}
}

// GetFlightInfo анализирует дескриптор и возвращает Ticket с фильтром.
//
// Дескрипторы (через path):
//
//	["windows"]               → Ticket: {"cmd":"all"}
//	["windows", "WB-001"]     → Ticket: {"cmd":"filter","product_id":"WB-001"}
func (s *FlightServer) GetFlightInfo(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	cmd := "all"
	productID := ""
	if len(desc.Path) >= 2 {
		cmd = "filter"
		productID = desc.Path[1]
	}

	ticket := EncodeTicket(cmd, productID)

	s.log.Info("FLIGHT_GET_FLIGHT_INFO",
		zap.String("path", fmt.Sprintf("%v", desc.Path)),
		zap.String("cmd", cmd),
		zap.String("product_id", productID),
	)

	schema := WindowSchema()
	info := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticket},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}
	return info, nil
}

// DoGet читает Ticket, применяет фильтр, стримит RecordBatch.
//
// Ticket JSON:
//
//	{"cmd":"all"}                  → все записи
//	{"cmd":"filter","product_id":"WB-001"}  → только WB-001
func (s *FlightServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	qt, err := ParseTicket(tkt.Ticket)
	if err != nil {
		s.log.Error("FLIGHT_BAD_TICKET", zap.String("raw", string(tkt.Ticket)), zap.Error(err))
		return fmt.Errorf("bad ticket: %w", err)
	}

	s.log.Info("FLIGHT_DO_GET",
		zap.String("cmd", qt.Cmd),
		zap.String("product_id", qt.ProductID),
	)

	// Фильтруем данные.
	var rows []models.WindowAgg
	switch qt.Cmd {
	case "filter":
		rows = s.store.SnapshotFiltered(qt.ProductID)
	default:
		rows = s.store.Snapshot()
	}

	if len(rows) == 0 {
		s.log.Warn("FLIGHT_NO_DATA", zap.String("filter", qt.ProductID))
		return nil
	}

	// Стримим.
	wr := flight.NewRecordWriter(stream, ipc.WithSchema(WindowSchema()))
	defer wr.Close()

	const batchSize = 1000
	var totalSent int64

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch, err := s.buildRecordBatch(rows[i:end])
		if err != nil {
			return fmt.Errorf("build batch: %w", err)
		}
		if err := wr.Write(batch); err != nil {
			batch.Release()
			return fmt.Errorf("write batch: %w", err)
		}
		batch.Release()
		totalSent += batch.NumRows()
	}

	s.log.Info("FLIGHT_DO_GET_COMPLETED",
		zap.String("filter", qt.ProductID),
		zap.Int64("rows", totalSent),
	)
	return nil
}

func (s *FlightServer) ListFlights(_ *flight.Criteria, server flight.FlightService_ListFlightsServer) error {
	s.log.Info("FLIGHT_LIST_FLIGHTS")
	// Возвращаем два потока: все данные и поштучный.
	for _, entry := range []struct{ path []string }{
		{[]string{"windows"}},
		{[]string{"windows", "<product_id>"}},
	} {
		desc := &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: entry.path,
		}
		info, err := s.GetFlightInfo(context.Background(), desc)
		if err != nil {
			return err
		}
		if err := server.Send(info); err != nil {
			return err
		}
	}
	return nil
}

func (s *FlightServer) GetSchema(_ context.Context, _ *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	raw := flight.SerializeSchema(WindowSchema(), s.alloc)
	return &flight.SchemaResult{Schema: raw}, nil
}

// ==========================================================================
// buildRecordBatch
// ==========================================================================

func (s *FlightServer) buildRecordBatch(rows []models.WindowAgg) (arrow.Record, error) {
	schema := WindowSchema()
	n := len(rows)
	pool := s.alloc

	strB := array.NewStringBuilder(pool)
	tsB := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"})
	f64B := array.NewFloat64Builder(pool)
	i64LB := array.NewInt64Builder(pool)
	i64CB := array.NewInt64Builder(pool)

	defer strB.Release()
	defer tsB.Release()
	defer f64B.Release()
	defer i64LB.Release()
	defer i64CB.Release()

	strB.Reserve(n)
	tsB.Reserve(n)
	f64B.Reserve(n)
	i64LB.Reserve(n)
	i64CB.Reserve(n)

	for _, r := range rows {
		strB.Append(r.ProductID)
		tsB.Append(arrow.Timestamp(r.WindowStart.UnixNano()))
		f64B.Append(r.AvgRating)
		i64LB.Append(int64(r.TotalLikes))
		i64CB.Append(int64(r.ReviewCount))
	}

	return array.NewRecord(schema, []arrow.Array{
		strB.NewArray(),
		tsB.NewArray(),
		f64B.NewArray(),
		i64LB.NewArray(),
		i64CB.NewArray(),
	}, int64(n)), nil
}

// ==========================================================================
// Server runner
// ==========================================================================

func Run(ctx context.Context, addr string, flightServer *FlightServer, logger *zap.Logger) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	grpcServer := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	flight.RegisterFlightServiceServer(grpcServer, flightServer)

	go func() {
		<-ctx.Done()
		logger.Info("FLIGHT_SERVER_SHUTTING_DOWN")
		grpcServer.GracefulStop()
	}()

	logger.Info("FLIGHT_SERVER_STARTED", zap.String("addr", addr))
	return grpcServer.Serve(lis)
}
