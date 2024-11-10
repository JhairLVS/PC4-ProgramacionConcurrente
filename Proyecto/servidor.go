package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Review struct {
	ReviewerID      string `json:"reviewer_id"`
	ProductID       string `json:"product_id"`
	Stars           int    `json:"stars"`
	ProductCategory string `json:"category"`
}

type Recommendation struct {
	ProductID string  `json:"product_id"`
	Score     float64 `json:"score"`
}

var userReviews = make(map[string][]Review)
var aggregatedRecommendations = make(map[string][]float64) // Almacena las puntuaciones de recomendaciones
var wg sync.WaitGroup

// Cargar el dataset
func loadDataset(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Error al abrir el archivo: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("Error al leer el archivo CSV: %v", err)
	}

	for _, record := range records[1:] {
		if len(record) < 8 { // Verifica que el registro tenga al menos 8 columnas
			fmt.Println("Registro incompleto o con formato incorrecto, saltando fila:", record)
			continue
		}

		stars, _ := strconv.Atoi(record[3]) // Índice de "stars"
		review := Review{
			ReviewerID:      record[2], // Índice de "reviewer_id"
			ProductID:       record[1], // Índice de "product_id"
			Stars:           stars,
			ProductCategory: record[7], // Índice de "product_category"
		}
		userReviews[review.ReviewerID] = append(userReviews[review.ReviewerID], review)
	}
	fmt.Printf("Total de datos cargados: %d\n", len(records)-1)
	return nil
}

// Asignación uniforme de datos a los clientes
func assignUniformBlocks(totalReviews []Review, numClients, clientID int) []Review {
	chunkSize := len(totalReviews) / numClients
	start := clientID * chunkSize
	end := start + chunkSize
	if clientID == numClients-1 {
		end = len(totalReviews)
	}
	return totalReviews[start:end]
}

// Agregar recomendaciones de un cliente al almacenamiento agregado
func aggregateRecommendations(recommendations []Recommendation) {
	for _, rec := range recommendations {
		aggregatedRecommendations[rec.ProductID] = append(aggregatedRecommendations[rec.ProductID], rec.Score)
	}
}

// Normalizar las puntuaciones al rango de 1 a 5
func normalizeScore(score float64, min float64, max float64) float64 {
	if score < min {
		return min
	}
	if score > max {
		return max
	}
	return score
}

// Algoritmo de Consenso Bayesiano con normalización de puntuaciones
func calculateBayesianConsensus() map[string]float64 {
	finalScores := make(map[string]float64)
	neutralScore := 3.0 // Media neutra

	for productID, scores := range aggregatedRecommendations {
		n := float64(len(scores))
		if n == 0 {
			continue
		}
		mean, variance := 0.0, 0.0

		// Calcula la media y varianza de las puntuaciones
		for _, score := range scores {
			mean += score
		}
		mean /= n
		for _, score := range scores {
			variance += (score - mean) * (score - mean)
		}
		variance /= n

		// Ajusta la puntuación usando un enfoque bayesiano hacia una media neutra
		weightedMean := (mean*n + neutralScore*variance) / (n + variance)

		// Normaliza la puntuación al rango de 1 a 5
		finalScores[productID] = normalizeScore(weightedMean, 1, 5)
	}
	return finalScores
}

// Convertir la puntuación a estrellas
func calculateStarRating(score float64) string {
	switch {
	case score >= 4.5:
		return "★★★★★"
	case score >= 3.5:
		return "★★★★☆"
	case score >= 2.5:
		return "★★★☆☆"
	case score >= 1.5:
		return "★★☆☆☆"
	default:
		return "★☆☆☆☆"
	}
}

// Mostrar el top 5 de recomendaciones con estrellas
func displayTopRecommendationsWithStars(finalScores map[string]float64) {
	type productScore struct {
		ProductID string
		Score     float64
	}

	var scoresList []productScore
	for productID, score := range finalScores {
		scoresList = append(scoresList, productScore{ProductID: productID, Score: score})
	}

	// Ordenar los productos por puntuación de forma descendente
	sort.Slice(scoresList, func(i, j int) bool {
		return scoresList[i].Score > scoresList[j].Score
	})

	fmt.Println("\nTop 5 productos recomendados:")
	for i := 0; i < 5 && i < len(scoresList); i++ {
		fmt.Printf("Producto: %s, Puntuación: %.2f, Estrellas: %s\n",
			scoresList[i].ProductID, scoresList[i].Score, calculateStarRating(scoresList[i].Score))
	}

	fmt.Println("\nBottom 5 productos menos recomendados:")
	for i := len(scoresList) - 1; i >= len(scoresList)-5 && i >= 0; i-- {
		fmt.Printf("Producto: %s, Puntuación: %.2f, Estrellas: %s\n",
			scoresList[i].ProductID, scoresList[i].Score, calculateStarRating(scoresList[i].Score))
	}
}

// Conexión con tiempo de espera y reintentos
func connectWithTimeout(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, fmt.Errorf("conexión fallida: %v", err)
	}
	return conn, nil
}

// Manejo de la conexión con el cliente con tolerancia a fallos
func handleClient(conn net.Conn, clientID, numClients int, totalReviews []Review, reassignCh chan int) {
	defer conn.Close()
	defer wg.Done()

	fmt.Printf("Cliente %d conectado desde %s\n", clientID, conn.RemoteAddr())

	reviews := assignUniformBlocks(totalReviews, numClients, clientID)
	data, err := json.Marshal(reviews)
	if err != nil {
		fmt.Println("Error al convertir datos a JSON:", err)
		return
	}
	data = append(data, '\n')
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second)) // Timeout de escritura
	_, writeErr := conn.Write(data)
	if writeErr != nil {
		fmt.Printf("Error al enviar datos al cliente %d: %v. Reasignando tarea...\n", clientID, writeErr)
		reassignCh <- clientID
		return
	}
	fmt.Printf("Datos enviados al cliente %d: %d reseñas\n", clientID, len(reviews))

	reader := bufio.NewReader(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(10 * time.Second)) // Timeout de lectura
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Conexión perdida con el cliente %d (%s): %v\n", clientID, conn.RemoteAddr(), err)
			reassignCh <- clientID
			return
		}

		if message == "FINISHED\n" {
			fmt.Printf("Cliente %d ha terminado el envío de recomendaciones.\n", clientID)
			return
		}
		fmt.Printf("Mensaje recibido: %s\n", message)
		var recommendations []Recommendation
		if err := json.Unmarshal([]byte(message), &recommendations); err != nil {
			fmt.Println("Error al decodificar JSON:", err)
			fmt.Println("Mensaje que causó el error:", message)
			return
		}
		aggregateRecommendations(recommendations)
		fmt.Printf("Recomendaciones recibidas en lote del cliente %d: %d\n", clientID, len(recommendations))
	}
}

var clientCounter int
var mu sync.Mutex // Mutex para asegurar que el contador de clientes sea consistente

func startServer() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		fmt.Println("Error al iniciar el servidor:", err)
		return
	}
	defer ln.Close()
	fmt.Println("Servidor escuchando en el puerto 9000...")

	totalReviews := []Review{}
	for _, reviews := range userReviews {
		totalReviews = append(totalReviews, reviews...)
	}

	numClients := 3
	reassignCh := make(chan int, numClients)            // Canal para reasignación de tareas
	availableClients := make(chan net.Conn, numClients) // Canal para clientes disponibles

	// Goroutine para gestionar la reasignación de tareas
	go func() {
		for clientID := range reassignCh {
			fmt.Printf("Reasignando tarea del cliente %d a otro cliente disponible...\n", clientID)
			// Intentar reasignar la tarea a un cliente disponible
			select {
			case newConn := <-availableClients:
				fmt.Printf("Reasignando tarea del cliente %d al cliente %s\n", clientID, newConn.RemoteAddr())
				// Crear una goroutine para manejar el nuevo cliente asignado
				wg.Add(1)
				go handleClient(newConn, clientID, numClients, totalReviews, reassignCh)
			default:
				fmt.Printf("No hay clientes disponibles en este momento para reasignar la tarea del cliente %d\n", clientID)
			}
		}
	}()

	// Loop principal de aceptación de clientes
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error al aceptar conexión:", err)
			continue
		}

		// Bloquear el acceso al contador de clientes para evitar conflictos concurrentes
		mu.Lock()
		clientID := clientCounter
		clientCounter++
		mu.Unlock()

		fmt.Printf("Cliente %d conectado desde %s\n", clientID, conn.RemoteAddr())

		// Agregar conexión al canal de clientes disponibles
		availableClients <- conn

		wg.Add(1)
		go handleClient(conn, clientID, numClients, totalReviews, reassignCh)
	}

	wg.Wait()
	close(reassignCh)
	close(availableClients)

	// Calcular el consenso bayesiano y mostrar las recomendaciones
	finalScores := calculateBayesianConsensus()
	displayTopRecommendationsWithStars(finalScores)
}

func main() {
	err := loadDataset("test.csv")
	if err != nil {
		fmt.Println("Error al cargar el dataset:", err)
		return
	}
	startServer()
}
