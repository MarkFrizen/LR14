package arrowserver

import (
	"context"
	"fmt"
	"net"
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
// Схема Arrow для WindowAgg
// ==========================================================================

// WindowSchema возвращает Arrow-схему для агрегированных окон.
//
// Колонки:
//
//	product_id    (Utf8)
//	window_start  (Timestamp_ns, UTC)
//	avg_rating    (Float64)
//	total_likes   (Int64)
//	review_count  (Int64)
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
// WindowStore — потокобезопасное хранилище WindowAgg для Flight-сервера
// ==========================================================================

// WindowStore хранит агрегированные окна в памяти.
type WindowStore struct {
	mu   sync.RWMutex
	rows []models.WindowAgg
	max  int // максимальное количество хранимых записей (0 = без лимита)
}

// NewWindowStore создаёт хранилище с опциональным лимитом.
func NewWindowStore(maxRecords int) *WindowStore {
	return &WindowStore{
		rows: make([]models.WindowAgg, 0, 1000),
		max:  maxRecords,
	}
}

// Push добавляет WindowAgg в хранилище. Если превышен лимит, удаляет самые старые.
func (s *WindowStore) Push(agg models.WindowAgg) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rows = append(s.rows, agg)
	if s.max > 0 && len(s.rows) > s.max {
		s.rows = s.rows[len(s.rows)-s.max:]
	}
}

// Snapshot возвращает копию всех хранящихся записей.
func (s *WindowStore) Snapshot() []models.WindowAgg {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cp := make([]models.WindowAgg, len(s.rows))
	copy(cp, s.rows)
	return cp
}

// Count возвращает количество записей.
func (s *WindowStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

// ==========================================================================
// Flight-сервер
// ==========================================================================

// FlightServer реализует сервер Arrow Flight RPC, который отдаёт WindowAgg
// в формате RecordBatch через метод DoGet ("GetWindows").
type FlightServer struct {
	flight.BaseFlightServer

	store *WindowStore
	log   *zap.Logger
	alloc memory.Allocator
}

// NewFlightServer создаёт новый Flight-сервер.
func NewFlightServer(store *WindowStore, logger *zap.Logger) *FlightServer {
	return &FlightServer{
		store: store,
		log:   logger,
		alloc: memory.NewGoAllocator(),
	}
}

// GetFlightInfo возвращает информацию о доступном Flight-потоке.
// Клиент вызывает этот метод, чтобы узнать, как получить данные.
func (s *FlightServer) GetFlightInfo(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s.log.Info("FLIGHT_GET_FLIGHT_INFO",
		zap.Any("descriptor_type", desc.Type),
		zap.String("path", fmt.Sprintf("%v", desc.Path)),
	)

	schema := WindowSchema()
	schemaBytes := flight.SerializeSchema(schema, s.alloc)

	// Ticket — произвольные байты, которые сервер интерпретирует в DoGet.
	// Используем команду "GET_WINDOWS".
	ticket := &flight.Ticket{Ticket: []byte("GET_WINDOWS")}

	// Endpoint указывает клиенту, куда подключаться для получения данных.
	// location можно оставить пустым, если клиент уже подключён к тому же серверу.
	info := &flight.FlightInfo{
		Schema:        schemaBytes,
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: ticket,
			// Location: — если нужно указать другой адрес
		}},
		TotalRecords: int64(s.store.Count()),
		TotalBytes:   -1,
	}

	s.log.Info("FLIGHT_INFO_SENT",
		zap.Int64("total_records", info.TotalRecords),
	)
	return info, nil
}

// DoGet — основной метод, который клиент вызывает для получения данных.
// Читает WindowAgg из store, упаковывает в Arrow RecordBatch и стримит.
func (s *FlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	s.log.Info("FLIGHT_DO_GET",
		zap.String("ticket", string(ticket.Ticket)),
	)

	// Берём снимок данных.
	rows := s.store.Snapshot()
	if len(rows) == 0 {
		s.log.Warn("FLIGHT_NO_DATA")
		return nil
	}

	// Создаём writer поверх gRPC-стрима.
	wr := flight.NewRecordWriter(stream, ipc.WithSchema(WindowSchema()))
	defer wr.Close()

	// Делим данные на батчи по 1000 записей (чтобы не создавать гигантский batch).
	const batchSize = 1000
	var totalSent int64

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch, err := s.buildRecordBatch(rows[i:end])
		if err != nil {
			s.log.Error("FLIGHT_BUILD_BATCH_ERROR", zap.Error(err))
			return fmt.Errorf("build record batch: %w", err)
		}

		if err := wr.Write(batch); err != nil {
			batch.Release()
			s.log.Error("FLIGHT_WRITE_BATCH_ERROR", zap.Error(err))
			return fmt.Errorf("write record batch: %w", err)
		}
		batch.Release()
		totalSent += batch.NumRows()

		s.log.Debug("FLIGHT_BATCH_SENT",
			zap.Int64("rows", batch.NumRows()),
			zap.Int64("total_sent", totalSent),
		)
	}

	s.log.Info("FLIGHT_DO_GET_COMPLETED",
		zap.Int64("total_rows", totalSent),
	)
	return nil
}

// ListFlights — заглушка, возвращающая список доступных потоков.
func (s *FlightServer) ListFlights(_ *flight.Criteria, server flight.FlightService_ListFlightsServer) error {
	s.log.Info("FLIGHT_LIST_FLIGHTS")
	// Можно вернуть информацию о единственном потоке "windows".
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"windows"},
	}
	info, err := s.GetFlightInfo(context.Background(), desc)
	if err != nil {
		return err
	}
	return server.Send(info)
}

// GetSchema возвращает схему данных.
func (s *FlightServer) GetSchema(_ context.Context, _ *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	schema := WindowSchema()
	schemaBytes := flight.SerializeSchema(schema, s.alloc)
	return &flight.SchemaResult{Schema: schemaBytes}, nil
}

// buildRecordBatch создаёт Arrow RecordBatch из слайса WindowAgg.
func (s *FlightServer) buildRecordBatch(rows []models.WindowAgg) (arrow.Record, error) {
	schema := WindowSchema()
	n := len(rows)

	// Создаём builder'ы для каждой колонки.
	pool := s.alloc
	strBuilder := array.NewStringBuilder(pool)
	tsBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"})
	f64Builder := array.NewFloat64Builder(pool)
	i64LikesBuilder := array.NewInt64Builder(pool)
	i64CountBuilder := array.NewInt64Builder(pool)

	defer strBuilder.Release()
	defer tsBuilder.Release()
	defer f64Builder.Release()
	defer i64LikesBuilder.Release()
	defer i64CountBuilder.Release()

	// Резервируем память.
	strBuilder.Reserve(n)
	tsBuilder.Reserve(n)
	f64Builder.Reserve(n)
	i64LikesBuilder.Reserve(n)
	i64CountBuilder.Reserve(n)

	for _, r := range rows {
		strBuilder.Append(r.ProductID)
		tsBuilder.Append(arrow.Timestamp(r.WindowStart.UnixNano()))
		f64Builder.Append(r.AvgRating)
		i64LikesBuilder.Append(int64(r.TotalLikes))
		i64CountBuilder.Append(int64(r.ReviewCount))
	}

	return array.NewRecord(schema, []arrow.Array{
		strBuilder.NewArray(),
		tsBuilder.NewArray(),
		f64Builder.NewArray(),
		i64LikesBuilder.NewArray(),
		i64CountBuilder.NewArray(),
	}, int64(n)), nil
}

// ==========================================================================
// Server runner
// ==========================================================================

// Run запускает gRPC-сервер с Flight-сервисом на указанном адресе.
func Run(ctx context.Context, addr string, flightServer *FlightServer, logger *zap.Logger) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	grpcServer := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
	)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)

	// Graceful shutdown: при отмене контекста останавливаем gRPC.
	go func() {
		<-ctx.Done()
		logger.Info("FLIGHT_SERVER_SHUTTING_DOWN")
		grpcServer.GracefulStop()
	}()

	logger.Info("FLIGHT_SERVER_STARTED",
		zap.String("addr", addr),
	)
	return grpcServer.Serve(lis)
}
