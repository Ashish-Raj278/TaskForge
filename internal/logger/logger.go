package logger

import (
	"TaskForge/internal/task"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func LogSuccess(cur_task task.Task) {
	f, err := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal("Error logging: ", err)
		return
	}
	defer f.Close()

	payload_str, er := json.Marshal(cur_task.Payload)
	if er != nil {
		payload_str = []byte{}
	}

	text := "\n SUCCESS: Task type: " + cur_task.Type + " Task payload: " + string(payload_str) + " Attempts: " + fmt.Sprintf("%d/%d", cur_task.Attempts, cur_task.MaxRetries)

	if _, err := f.WriteString(text); err != nil {
		log.Fatal("Error writing to the log file: ", err)
		return
	}
	log.Println("logged successfully to the file")
}

func LogFailure(cur_task task.Task, cur_err error) {

	f, err := os.OpenFile("/TaskForge/logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal("Error logging: ", err)
		return
	}
	defer f.Close()

	payload_str, er := json.Marshal(cur_task.Payload)
	if er != nil {
		payload_str = []byte{}
	}

	text := "FAILURE: Task type: " + cur_task.Type + " Task payload: " + string(payload_str) + " Attempts: " + fmt.Sprintf("%d/%d", cur_task.Attempts, cur_task.MaxRetries) + " Error message: " + cur_err.Error()

	if _, err := f.WriteString(text); err != nil {
		log.Fatal("Error writing to the log file: ", err)
		return
	}
	log.Println("logged successfully to the file")
}
