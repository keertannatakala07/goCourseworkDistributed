package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type CalculateTurnService struct {
	mu             sync.Mutex
	world          [][]byte
	completedTurns int
	height         int
	width          int
	paused         bool
	mustQuit       bool
	listener       net.Listener
}

type Request struct {
	WorldSlice  [][]byte
	Turns       int
	ImageHeight int
	ImageWidth  int
}

type Response struct {
	NewWorldSlice  [][]byte
	CompletedTurns int
	AliveCells     int
}

type KeyPressRequest struct {
	Key rune
}

type KeyPressResponse struct {
	Paused         bool
	SaveRequested  bool
	SaveWorld      [][]byte
	CompletedTurns int
}

type EmptyArgs struct{}

type HaloRequest struct {
	Row []byte
}

type HaloResponse struct {
	Row []byte
}

type WorkerSliceRequest struct {
	GolSlice        [][]byte
	Turns           int
	ImageWidth      int
	Height          int
	WorkerIndex     int
	NumberOfWorkers int
	AboveWorker     string
	BelowWorker     string
}

type WorkerSliceResponse struct {
	NewGolSlice    [][]byte
	CompletedTurns int
	AliveCells     int
}

func (ct *CalculateTurnService) ShareHalo(req HaloRequest, res *HaloResponse) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	res.Row = req.Row
	return nil
}

func (ct *CalculateTurnService) GetBottomRow(args EmptyArgs, res *HaloResponse) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	res.Row = ct.world[len(ct.world)-1]
	return nil
}

func (ct *CalculateTurnService) GetTopRow(args EmptyArgs, res *HaloResponse) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	res.Row = ct.world[0]
	return nil
}

func nextStateRow(rowIndex int, width int, world [][]byte, aboveHalo, belowHalo []byte, height int) []byte {
	newRow := make([]byte, width)
	for i := 0; i < width; i++ {
		aliveCells := 0

		for dr := -1; dr <= 1; dr++ {
			for dc := -1; dc <= 1; dc++ {
				if dr == 0 && dc == 0 {
					continue
				}

				newColumn := (i + dc + width) % width
				newRow := rowIndex + dr

				var cellValue byte

				if newRow < 0 {
					if aboveHalo != nil {
						cellValue = aboveHalo[newColumn]
					}
				} else if newRow >= height {
					if belowHalo != nil {
						cellValue = belowHalo[newColumn]
					}
				} else {
					cellValue = world[newRow][newColumn]
				}

				if cellValue == 255 {
					aliveCells++
				}
			}
		}

		if world[rowIndex][i] == 255 {
			if aliveCells == 2 || aliveCells == 3 {
				newRow[i] = 255
			} else {
				if aliveCells == 3 {
					newRow[i] = 255

				}
			}
		}
	}
	return newRow

}

func calculateNextState(height, width int, world [][]byte) [][]byte {
	newWorld := make([][]byte, height)
	for i := range newWorld {
		newWorld[i] = make([]byte, width)
	}

	for row := 0; row < height; row++ {
		for column := 0; column < width; column++ {
			liveCells := 0
			for dr := -1; dr <= 1; dr++ {
				for dc := -1; dc <= 1; dc++ {
					if dr == 0 && dc == 0 {
						continue
					}
					nRow := (row + dr + height) % height
					nCol := (column + dc + width) % width
					if world[nRow][nCol] == 255 {
						liveCells++
					}
				}
			}
			if world[row][column] == 255 {
				if liveCells == 2 || liveCells == 3 {
					newWorld[row][column] = 255
				}
			} else if liveCells == 3 {
				newWorld[row][column] = 255
			}
		}
	}
	return newWorld
}

func countAliveCells(height, width int, world [][]byte) int {
	count := 0
	for row := 0; row < height; row++ {
		for column := 0; column < width; column++ {
			if world[row][column] == 255 {
				count++
			}
		}
	}
	return count
}

func (ct *CalculateTurnService) CalculateTurns(request WorkerSliceRequest, response *WorkerSliceResponse) error {
	fmt.Println("Worker %d: Accepted request for (rows: %d)\n")

	ct.mu.Lock()
	ct.world = request.GolSlice
	height := len(ct.world)
	ct.height = height
	ct.width = request.ImageWidth
	ct.completedTurns = 0
	ct.paused = false
	ct.mustQuit = false

	ct.mu.Unlock()

	for i := 0; i < request.Turns; i++ {
		ct.mu.Lock()
		for ct.paused && !ct.mustQuit {
			ct.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			ct.mu.Lock()
		}
		if ct.mustQuit {
			ct.mu.Unlock()
			break
		}
		ct.mu.Unlock()

		var aboveHalo, belowHalo []byte

		if request.AboveWorker != "" {
			client, err := rpc.Dial("tcp", request.AboveWorker)
			if err == nil {
				var haloRes HaloResponse
				err := client.Call("CalculateTurnService.GetBottomRow", EmptyArgs{}, &haloRes)
				if err == nil {
					aboveHalo = haloRes.Row
				}
				client.Close()
			}
		}

		if request.BelowWorker != "" {
			client, err := rpc.Dial("tcp", request.BelowWorker)
			if err == nil {
				var haloRes HaloResponse
				err := client.Call("CalculateTurnService.GetTopRow", EmptyArgs{}, &haloRes)
				if err == nil {
					belowHalo = haloRes.Row
				}
				client.Close()
			}
		}
		newWorld := make([][]byte, height)
		for i := 0; i < height; i++ {
			newWorld[i] = nextStateRow(i, request.ImageWidth, ct.world, aboveHalo, belowHalo, height)
		}

		ct.mu.Lock()
		ct.world = newWorld
		ct.completedTurns = i + 1
		ct.mu.Unlock()
	}

	ct.mu.Lock()
	response.NewGolSlice = ct.world
	response.CompletedTurns = ct.completedTurns
	response.AliveCells = countAliveCells(height, request.ImageWidth, ct.world)
	ct.mu.Unlock()

	fmt.Printf("Worker %d Processed %d turns\n", request.WorkerIndex, response.CompletedTurns)
	return nil
}

func (ct *CalculateTurnService) HandleKeyPress(request KeyPressRequest, response *KeyPressResponse) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	switch request.Key {
	case 's':
		response.SaveRequested = true
		response.CompletedTurns = ct.completedTurns
		response.SaveWorld = make([][]byte, len(ct.world))
		for i := range ct.world {
			response.SaveWorld[i] = make([]byte, len(ct.world[i]))
			copy(response.SaveWorld[i], ct.world[i])
		}
	case 'p':
		ct.paused = !ct.paused
		response.Paused = ct.paused
		response.CompletedTurns = ct.completedTurns
	case 'q':
		ct.mustQuit = true
		ct.paused = false
		response.SaveRequested = true
		response.CompletedTurns = ct.completedTurns
		response.Paused = false
		response.SaveWorld = make([][]byte, len(ct.world))
		for i := range ct.world {
			response.SaveWorld[i] = make([]byte, len(ct.world[i]))
			copy(response.SaveWorld[i], ct.world[i])
		}
	case 'k':
		ct.mustQuit = true
		ct.paused = false
		response.SaveRequested = true
		response.CompletedTurns = ct.completedTurns
		response.Paused = false
		response.SaveWorld = make([][]byte, len(ct.world))
		for i := range ct.world {
			response.SaveWorld[i] = make([]byte, len(ct.world[i]))
			copy(response.SaveWorld[i], ct.world[i])
		}

		go func() {
			time.Sleep(50 * time.Millisecond)
			if ct.listener != nil {
				ct.listener.Close()
			}
		}()
	default:
		response.CompletedTurns = ct.completedTurns
	}

	return nil
}

func (ct *CalculateTurnService) GetAliveCells(args *EmptyArgs, response *Response) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if ct.world == nil {
		response.CompletedTurns = 0
		response.AliveCells = 0
		return nil
	}

	response.CompletedTurns = ct.completedTurns
	response.AliveCells = countAliveCells(ct.height, ct.width, ct.world)
	return nil
}

func main() {
	gob.Register([][]byte{})
	port := flag.String("port", "8080", "worker listen on")
	flag.Parse()

	service := new(CalculateTurnService)
	err := rpc.Register(service)
	if err != nil {
		fmt.Println("Failed to register RPC: ", err)
	}

	address := "localhost:" + *port
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to listen: ", err)
		return
	}
	defer listener.Close()

	service.listener = listener

	fmt.Printf("Listening on %s...\n", address)
	rpc.Accept(listener)
}
