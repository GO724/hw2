package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// task:

// идем в "базу" чтоб получить user_id из email'а
// каждый запрос занимает 1 секунду
// можно без проблем выполнять параллельно
// func GetUser(email string) (res User)

// идем за списком писем
// каждый запрос занимает 1 секунду
// это API поддерживает батчи. то есть можно запросить за 1 вызов сразу информацию по нескольким юзерам
// GetMessagesMaxUsersBatch - максимальное кол-во юзеров, которое можно передать за 1 раз передать
// func GetMessages(users ...User) (res []MsgID, err error)

// идем в антиспам
// каждый запрос занимает 100мс
// у него есть антибрут. то есть если запрашивать параллельно слишком часто, то дает "по рукам" и возвращает ошибку
// HasSpamMaxAsyncRequests - максимальное кол-во параллельных запросов
// func HasSpam(id MsgID) (res bool, err error)

// написание функции `RunPipeline` которая обеспечивает нам конвейерную обработку функций-воркеров, которые что-то делают.
// type cmd func(in, out chan interface{})
// RunPipeline(
// 	cmd(newCatStrings(inputData, 0)),		инициализация джобы, которая просто выплюнет в out подряд все из слайса строк strs
// 	cmd(SelectUsers),						моя функция SelectUsers(in, out chan interface{})
// 	cmd(newCollectStrings(&testResult)),	инициализация джобы, которая считает из in все строки, пока канал не закроется. и положит все в strs
// )

// common.go

// type User struct {
// 	ID    uint64	MsgID??? uint64	crc64.Checksum([]byte(remail), crc64.MakeTable(crc64.ISO))
// 	Email string
// }

// type MsgData struct {	out of CheckSpam(in, out chan interface{}) ; in CombineResults(in, out chan interface{})
// 	ID      MsgID	uint64
// 	HasSpam bool
// }

// RunPipeline(cmds ...cmd)
//   - `in` - type cmd func(in, out chan interface{}). это функции конвеера
//
// алгоритм : cat emails.txt | SelectUsers | SelectMessages | CheckSpam | CombineResults
//
// SelectUsers(in, out chan interface{}) //	то есть мы получаем на вход имейлы юзеров, селектим их из "базы" и получаем каждому юзеру user_id
//
// SelectMessages(in, out chan interface{}) //	затем селектим по каждому user_id список писем(msg_id) этого юзера.
//
// CheckSpam(in, out chan interface{}) //	дальше проверяем эти письма на спам
//
// CombineResults(in, out chan interface{}) //	и выдаем итоговый результат: какие у письма со спамом, а какие нет.
//
// В чем подвох:
//   - из-за описанных выше особенностей у вас либо не будет асинхрона и функции последовательно будут выполняться слишком долго
//   - либо вы можете наоборот безконтрольно все распаралелить и тогда будут ошибки тк нарветесь на антибрут
//   - либо вы не соптимизируете запросы "батчами" и будете лишний раз вызывать функции
//   - на все расчеты у нас 3 сек. вообще суммарно по всем слипам должно быть 2.9сек,
//     но в тестах округлил до 3секунд, чтоб сгладить погрешности рандома и самих вычислений. при этом обращаю внимание,
//     что абсолютно верный код при запуске на винде без wsl может работать и немного дольше 3сек и тесты будут падать.
func RunPipeline(cmds ...cmd) {
	var in1 chan interface{} = nil
	var out1 chan interface{}
	var out2 chan interface{}
	// var out3 chan interface{}
	// var out4 chan interface{}
	// var out5 chan interface{}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func(c cmd, in, out chan interface{}) {
		fmt.Println("go reader")
		defer wg.Done()
		c(in, out)
		// Закрываем выходной канал после завершения команды
		// if out != nil {
		// 	close(out)
		// }
	}(cmds[1], out1, out2) // reader

	time.Sleep(time.Second)

	wg.Add(1)
	go func(c cmd, in, out chan interface{}) {
		fmt.Println("go writer")
		defer wg.Done()
		c(in, out)
		// Закрываем выходной канал после завершения команды
		// if out != nil {
		// 	close(out)
		// }
	}(cmds[0], in1, out1) // writer

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	cmds[2](out2, out3) // SelectMessages(outSelectUsers, outSelectMessages)
	// }()

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	cmds[3](out3, out4) // CheckSpam(outSelectMessages, outCheckSpam)
	// }()

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	cmds[4](out4, out5) // CombineResults(outCheckSpam, outCombineResults)
	// }()

	wg.Wait()

	// wg := new(sync.WaitGroup)
	// var in chan interface{} = nil
	// ch := make([]chan interface{}, len(cmds))
	// for i, cmd := range cmds {
	// 	ch[i] = make(chan interface{})
	// 	switch i {
	// 	case 0:
	// 		wg.Add(1)
	// 		go func() {
	// 			defer wg.Done()
	// 			cmd(in, ch[i])
	// 		}()
	// 		continue

	// 		// case len(cmds):
	// 		// 	outN := make(chan interface{})
	// 		// 	go cmd()
	// 		// 	continue

	// 	}
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		cmd(ch[i-1], ch[i])
	// 	}()
	// }

}

// SelectUsers(SelectUsers(in, out chan interface{}))
//   - `in` - `string` внутри `interface{}`. это имейлы юзеров
//   - `out` - отдает структурки `User{}`. это результат функции `GetUser()`
//   - особенности:
//   - `GetUser()` выполняется 1 секунду. его можно вызывать параллельно для нескольких юзеров. это экономит время
//   - у некоторых юзеров есть alias'ы(псевдонимы). то есть на вид это два разных имейла, но на самом деле это один и тот же юзер в базе.
//     например "batman@mail.ru" - это алиас к "bruce.wayne@mail.ru". `SelectUsers()` должен отдавать в `out` только уникальных юзеров.
func SelectUsers(in, out chan interface{}) {
	// 	in - string
	// 	out - User

	wg := new(sync.WaitGroup)

	result := make(map[User]struct{})
	for iVal := range in {
		email, err := iString(iVal)
		if err != nil {
			fmt.Println(err)
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			u := GetUser(email)

			// check unique user
			if _, ok := result[u]; !ok {
				result[u] = struct{}{}
				// put out only unique user
				out <- u
			}
		}()

	}

	go func() {
		fmt.Println("SelectUsers.close(out)")
		wg.Wait()
		close(out)
	}()

}

// SelectMessages(SelectMessages(in, out chan interface{}))
//   - `in` -  `User{}` внутри `interface{}` от `SelectUsers()`
//   - `out` - отдает `MsgID`. это айдишники писем юзеров - результат функции `GetMessages()`
//   - особенности:
//   - `GetMessages()` выполняется 1 секунду. его тоже можно вызывать параллельно.
//   - но `GetMessages()` позволяет использовать "батчи".
//     то есть за раз в нее можно запихнуть не 1 юзера, а несколько. максимальное кол-во юзеров = 2.
//     то есть если мы хотим селектнуть у 10ти юзеров письма, то это можно сделать за 5 вызовов `GetMessages()`.
//     в тестах проверяется, что кол-во вызовов оптимальное
func SelectMessages(in, out chan interface{}) {
	// 	in - User
	// 	out - MsgID

	wg := new(sync.WaitGroup)
	users := make([]User, 0, 2)
	// msgIDs := make([]MsgID, 0, 100)

	for iVal := range in {
		user, err := iUser(iVal)
		if err != nil {
			fmt.Println(err)
			continue
		}

		users = append(users, user) // make batch

		if len(users) == 2 {
			wg.Add(1)
			go sendBatch(users, out, wg)
			users = users[0:0:2] // reset batch
		}
	}

	if len(users) > 0 {
		wg.Add(1)
		go sendBatch(users, out, wg)
	}

	go func() {
		wg.Wait()
		fmt.Println("SelectMessages.close(out)")
		close(out)
	}()

}

func sendBatch(users []User, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	msgIDs, err := GetMessages(users...)

	if err != nil {
		panic(err)
	}

	// result to out chan
	for _, msgID := range msgIDs {
		out <- msgID
	}
}

// CheckSpam(in, out chan interface{})
//   - `in` - `MsgID` внутри `interface{}`. это айдишники писем
//   - `out` - `MsgData{}`. это структура с парой полей: id и факт того является ли письмо спамом. это результат работы `HasSpam()`
//   - особенности:
//   - `HasSpam()` симулирует поход в сервис антиспама, чтоб проверить письмо на наличие спама.
//     один запрос выполняется за 100мс.
//     и у этого сервиса есть "антибрут" - его нельзя вызывать бесконтрольно в кучу потоков.
//     если сделать к нему более 5 параллельных запросов, то он начнет возвращать ошибку и данные о наличии спама вы не получите.
func CheckSpam(in, out chan interface{}) {
	// in - MsgID
	// out - MsgData

	wg := new(sync.WaitGroup)

	sem := NewSemaphore(5)

	for iVal := range in {
		// limit 5 parallel
		msgID, err := iMsgID(iVal)
		if err != nil {
			panic(err)
		}
		wg.Add(1)
		go func(msgID MsgID) {
			sem.Acquire()
			defer wg.Done()
			defer sem.Release()
			isSpam, err := HasSpam(msgID)
			if err != nil {
				fmt.Printf("**********ANTIBRUT**********%s**********ANTIBRUT**********\n", err)
			}
			out <- MsgData{
				ID:      msgID,
				HasSpam: isSpam,
			}

		}(msgID)

	}

	go func() {
		wg.Wait()
		fmt.Println("CheckSpam.close(out)")
		close(out)
	}()

}

type Semaphore struct {
	C chan struct{}
}

func NewSemaphore(limit int) *Semaphore {
	return &Semaphore{
		C: make(chan struct{}, limit),
	}
}

func (s *Semaphore) Acquire() {
	s.C <- struct{}{}
}

// release lock
func (s *Semaphore) Release() {
	<-s.C
}

// CombineResults(CombineResults(in, out chan interface{}))
//   - `in` - `MsgData` внутри `interface{}`
//   - `out` - `string`. это строки вида "<has_spam> <msg_id>", например "true 17696166526272393238"
//   - особенности:
//   - `CombineResults()` ждет все результаты из `in`, а потом сортирует их по наличию спама и по msg_id. то есть пример вывода может быть такой:
//     true 123
//     true 5555
//     true 5556
//     false 140
//     false 3000
//     false 3005
func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string
	defer close(out)

	result := make([]MsgData, 0, 100)
	for iVal := range in {
		msgData, err := iMsgData(iVal)
		if err != nil {
			panic(err)
		}
		result = append(result, msgData)
	}

	// sort slice

	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.HasSpam != b.HasSpam {
			return a.HasSpam && !b.HasSpam
		}
		return a.ID < b.ID
	})

	for _, msgData := range result {
		line := fmt.Sprintf("%5t %d\n", msgData.HasSpam, msgData.ID)
		fmt.Print(line)
		//out <- line
	}
}

func iString(i interface{}) (string, error) {
	switch v := i.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("неожиданный тип: %T", v)
	}
}

func iUser(i interface{}) (User, error) {
	switch v := i.(type) {
	case User:
		return v, nil
	default:
		return User{}, fmt.Errorf("неожиданный тип: %T", v)
	}
}

func iMsgID(i interface{}) (MsgID, error) {
	switch v := i.(type) {
	case MsgID:
		return v, nil
	default:
		return 0, fmt.Errorf("неожиданный тип: %T", v)
	}
}

func iMsgData(i interface{}) (MsgData, error) {
	switch v := i.(type) {
	case MsgData:
		return v, nil
	default:
		return MsgData{}, fmt.Errorf("неожиданный тип: %T", v)
	}
}

func main() {
	inputData := []string{
		"harry.dubois@mail.ru",
		"k.kitsuragi@mail.ru",
		"d.vader@mail.ru",
		"noname@mail.ru",
		"e.musk@mail.ru",
		"spiderman@mail.ru", // is an alias for peter.parker@mail.ru
		"red_prince@mail.ru",
		"tomasangelo@mail.ru",
		"batman@mail.ru", // is an alias for bruce.wayne@mail.ru
		"bruce.wayne@mail.ru",
	}

	inGenerator := make(chan interface{})
	outGenerator := make(chan interface{})

	go new2CatStrings(inputData, 0)(inGenerator, outGenerator)

	timeStart := time.Now()

	outSelectUsers := make(chan interface{})
	go SelectUsers(outGenerator, outSelectUsers)
	//readCh(outSelectUsers, 0)

	outSelectMessages := make(chan interface{})
	go SelectMessages(outSelectUsers, outSelectMessages)
	// readCh(outSelectMessages, 0)

	outCheckSpam := make(chan interface{})
	go CheckSpam(outSelectMessages, outCheckSpam)
	// readCh(outCheckSpam, 0)

	outCombineResults := make(chan interface{})
	CombineResults(outCheckSpam, outCombineResults)

	timeEnd := time.Since(timeStart)
	fmt.Println(timeEnd)

}

func readCh(in chan interface{}, delayMillisecond int) {
	fmt.Println("\t***** read ch started *****")
	for i := range in {
		if delayMillisecond != 0 {
			time.Sleep(time.Duration(delayMillisecond) * time.Millisecond)
		}
		switch v := i.(type) {
		default:
			fmt.Printf("\treadCh : %T %v\n", v, v)
		}
	}
	fmt.Println("\t***** read ch done *****")
}

// инициализация джобы, которая просто выплюнет в out подряд все из слайса строк strs
func new2CatStrings(strs []string, pauses time.Duration) func(in, out chan interface{}) {
	return func(in, out chan interface{}) {
		fmt.Println("start generator")
		for _, email := range strs {
			out <- email
			if pauses != 0 {
				time.Sleep(pauses)
			}
		}
		fmt.Println("stop generator: new2CatStrings.close(out)")
		close(out)
	}
}
