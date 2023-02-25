package main

import (
	"context"
	"fmt"
	"log"
	"os"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	_mongoMessagesCollection = "messages"
)

func main() {
	tgBotToken := os.Getenv("TG_BOT_TOKEN")
	if tgBotToken == "" {
		panic("empty TG_BOT_TOKEN")
	}
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		panic("empty DB_NAME")
	}
	dbUser := os.Getenv("DB_USER")
	if dbUser == "" {
		panic("empty DB_USER")
	}
	dbPassword := os.Getenv("DB_PASSWORD")
	if dbPassword == "" {
		panic("empty DB_PASSWORD")
	}
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		panic("empty DB_URL")
	}

	bot, err := tgbotapi.NewBotAPI(tgBotToken)
	if err != nil {
		log.Panic(err)
	}

	opts := options.
		Client().
		SetAuth(options.Credential{
			Username: dbUser,
			Password: dbPassword,
		}).
		ApplyURI(dbURL)

	client, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		log.Panic(err)
	}
	collection := client.Database(dbName).Collection(_mongoMessagesCollection)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	log.Println("Start reading updates")

	for update := range updates {
		if update.Message != nil {
			msg := update.Message

			if msg.Text == "/stata" {

				matchStage := bson.D{{"$match", bson.D{{"chat.id", msg.Chat.ID}}}}
				groupStage := bson.D{{"$group", bson.D{{"_id", "$from.username"}, {"total", bson.D{{"$sum", 1}}}}}}
				sortStage := bson.D{{"$sort", bson.D{{"total", -1}}}}

				cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{matchStage, groupStage, sortStage})
				if err != nil {
					fmt.Println(err)
					continue
				}
				var stats []bson.M
				err = cursor.All(context.Background(), &stats)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Println(stats)
				text := "Статистика по чату:\n"
				for i, stat := range stats {
					text += fmt.Sprintf("%d. %s: %d\n", i+1, stat["_id"], stat["total"])
				}

				nMsg := tgbotapi.NewMessage(msg.Chat.ID, text)
				nMsg.ReplyToMessageID = msg.MessageID
				bot.Send(nMsg)
			} else {
				_, err := collection.InsertOne(context.Background(), msg)
				if err != nil {
					log.Printf("[error] unable to insert message, charID:%d", msg.Chat.ID)
				}
			}
		}
	}
}
