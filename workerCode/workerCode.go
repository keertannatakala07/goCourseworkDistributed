package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type CalculateTurnService struct {
	mu             sync.Mutex
	world          [][]byte
	completedTurns int
	height         int
	width          int
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

type EmptyArgs struct{}

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

func (ct *CalculateTurnService) CalculateTurns(request Request, response *Response) error {
	fmt.Println("Accepted CalculateTurns request")

	ct.mu.Lock()
	ct.world = request.WorldSlice
	ct.height = request.ImageHeight
	ct.width = request.ImageWidth
	ct.completedTurns = 0
	ct.mu.Unlock()

	for i := 0; i < request.Turns; i++ {
		ct.mu.Lock()
		ct.world = calculateNextState(ct.height, ct.width, ct.world)
		ct.completedTurns = i + 1
		ct.mu.Unlock()
	}

	ct.mu.Lock()
	response.NewWorldSlice = ct.world
	response.CompletedTurns = ct.completedTurns
	response.AliveCells = countAliveCells(ct.height, ct.width, ct.world)
	ct.mu.Unlock()

	fmt.Printf("Processed %d turns for %dx%d grid\n", response.CompletedTurns, request.ImageWidth, request.ImageHeight)
	return nil
}

// GetAliveCells handles the ticker request from the local controller every 2 seconds
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
	service := new(CalculateTurnService)
	err := rpc.Register(service)
	if err != nil {
		fmt.Println("Failed to register RPC: ", err)
	}

	listener, err := net.Listen("tcp", "0.0.0.0:8080")
	if err != nil {
		fmt.Println("Failed to listen: ", err)
	}
	defer listener.Close()

	fmt.Println("Listening on 0.0.0.0:8080...")
	rpc.Accept(listener)
}
