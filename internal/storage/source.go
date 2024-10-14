package storage

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"news-feed-bot/internal/model"
)

type SourcePostgresStorage struct {
	db *sqlx.DB
}

func NewSourceStorage(db *sqlx.DB) *SourcePostgresStorage {
	return &SourcePostgresStorage{db: db}
}

func (s *SourcePostgresStorage) Sources(ctx context.Context) ([]model.Source, error) {
	// Нет необходимости явно запрашивать соединение через Connx, используем s.db напрямую
	var sources []dbSource
	if err := s.db.SelectContext(ctx, &sources, `SELECT * FROM sources`); err != nil {
		return nil, err
	}

	// Используйте явное преобразование
	return lo.Map(sources, func(source dbSource, _ int) model.Source {
		return model.Source{
			ID:        source.ID,
			Name:      source.Name,
			FeedURL:   source.FeedURL,
			Priority:  source.Priority,
			CreatedAt: source.CreatedAt,
		}
	}), nil
}

func (s *SourcePostgresStorage) SourceByID(ctx context.Context, id int64) (*model.Source, error) {
	var source dbSource
	if err := s.db.GetContext(ctx, &source, `SELECT * FROM sources WHERE id = $1`, id); err != nil {
		return nil, err
	}

	// Возвращаем преобразованный объект
	return &model.Source{
		ID:        source.ID,
		Name:      source.Name,
		FeedURL:   source.FeedURL,
		Priority:  source.Priority,
		CreatedAt: source.CreatedAt,
	}, nil
}

func (s *SourcePostgresStorage) Add(ctx context.Context, source model.Source) (int64, error) {
	// Запрос напрямую на s.db
	var id int64
	row := s.db.QueryRowxContext(
		ctx,
		`INSERT INTO sources (name, feed_url, priority)
		 VALUES ($1, $2, $3) RETURNING id;`,
		source.Name, source.FeedURL, source.Priority,
	)

	// Сначала проверяем ошибку от QueryRowxContext
	if err := row.Err(); err != nil {
		return 0, err
	}

	// Далее сканируем id
	if err := row.Scan(&id); err != nil {
		return 0, err
	}

	return id, nil
}

func (s *SourcePostgresStorage) SetPriority(ctx context.Context, id int64, priority int) error {
	// Обновляем приоритет напрямую через s.db
	_, err := s.db.ExecContext(ctx, `UPDATE sources SET priority = $1 WHERE id = $2`, priority, id)
	return err
}

func (s *SourcePostgresStorage) Delete(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sources WHERE id = $1`, id)
	return err
}

// Внутренняя структура для работы с БД
type dbSource struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	FeedURL   string    `db:"feed_url"`
	Priority  int       `db:"priority"`
	CreatedAt time.Time `db:"created_at"`
}
