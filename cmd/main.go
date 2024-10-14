package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"news-feed-bot/internal/bot"
	"news-feed-bot/internal/bot/middleware"
	"news-feed-bot/internal/botkit"
	"news-feed-bot/internal/config"
	"news-feed-bot/internal/fetcher"
	"news-feed-bot/internal/notifier"
	"news-feed-bot/internal/storage"
	"news-feed-bot/internal/summary"
)

func main() {
	botAPI, err := tgbotapi.NewBotAPI(config.Get().TelegramBotToken)
	if err != nil {
		log.Printf("[ERROR] failed to create botAPI: %v", err)
		return
	}

	db, err := sqlx.Connect("postgres", config.Get().DatabaseDSN)
	if err != nil {
		log.Printf("[ERROR] failed to connect to db: %v", err)
		return
	}
	defer func(db *sqlx.DB) {
		err := db.Close()
		if err != nil {

		}
	}(db)

	var (
		articleStorage = storage.NewArticleStorage(db)
		sourceStorage  = storage.NewSourceStorage(db)
		fetcher        = fetcher.New(
			articleStorage,
			sourceStorage,
			config.Get().FetchInterval,
			config.Get().FilterKeywords,
		)
		summarizer = summary.NewOpenAISummarizer(
			config.Get().OpenAIKey,
			config.Get().OpenAIModel,
			config.Get().OpenAIPrompt,
		)
		notifier = notifier.New(
			articleStorage,
			summarizer,
			botAPI,
			config.Get().NotificationInterval,
			2*config.Get().FetchInterval,
			config.Get().TelegramChannelID,
		)
	)

	newsBot := botkit.New(botAPI)
	newsBot.RegisterCmdView(
		"addsource",
		middleware.AdminsOnly(
			config.Get().TelegramChannelID,
			bot.ViewCmdAddSource(sourceStorage),
		),
	)
	newsBot.RegisterCmdView(
		"setpriority",
		middleware.AdminsOnly(
			config.Get().TelegramChannelID,
			bot.ViewCmdSetPriority(sourceStorage),
		),
	)
	newsBot.RegisterCmdView(
		"getsource",
		middleware.AdminsOnly(
			config.Get().TelegramChannelID,
			bot.ViewCmdGetSource(sourceStorage),
		),
	)
	newsBot.RegisterCmdView(
		"listsources",
		middleware.AdminsOnly(
			config.Get().TelegramChannelID,
			bot.ViewCmdListSource(sourceStorage),
		),
	)
	newsBot.RegisterCmdView(
		"deletesource",
		middleware.AdminsOnly(
			config.Get().TelegramChannelID,
			bot.ViewCmdDeleteSource(sourceStorage),
		),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func(ctx context.Context) {
		if err := fetcher.Start(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("[ERROR] failed to run fetcher: %v", err)
				return
			}

			log.Printf("[INFO] fetcher stopped")
		}
	}(ctx)

	go func(ctx context.Context) {
		if err := notifier.Start(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("[ERROR] failed to run notifier: %v", err)
				return
			}

			log.Printf("[INFO] notifier stopped")
		}
	}(ctx)

	go func(ctx context.Context) {
		if err := http.ListenAndServe("9.0.0.0:8080", mux); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("[ERROR] failed to run http server: %v", err)
				return
			}

			log.Printf("[INFO] http server stopped")
		}
	}(ctx)

	if err := newsBot.Run(ctx); err != nil {
		log.Printf("[ERROR] failed to run botkit: %v", err)
	}
}
