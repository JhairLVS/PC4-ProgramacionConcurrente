package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"sort"
	"time"
)

type Review struct {
	ReviewerID string `json:"reviewer_id"`
	ProductID  string `json:"product_id"`
	Stars      int    `json:"stars"`
	Category   string `json:"category"`
}

type Recommendation struct {
	ProductID string  `json:"product_id"`
	Score     float64 `json:"score"`
}

var batchSize = 5 // Tamaño del lote para enviar recomendaciones en partes

func main() {
	fmt.Println("===== Cliente del Sistema de Recomendación Distribuido =====")
	conn, err := connectToServer("localhost:9000")
	if err != nil {
		fmt.Println("Error al conectar con el servidor:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Conexión exitosa con el servidor en localhost:9000\n")

	// Recibir datos JSON del servidor
	data, err := receiveData(conn)
	if err != nil {
		fmt.Println("Error al recibir datos del servidor:", err)
		return
	}

	reviews := []Review{}
	err = json.Unmarshal(data, &reviews)
	if err != nil {
		fmt.Println("Error al decodificar JSON:", err)
		return
	}

	fmt.Printf("Cantidad de reseñas recibidas del servidor: %d\n", len(reviews))

	// Calcular recomendaciones concurrentemente usando ALS y similitud de coseno
	recommendations := calculateDistributedRecommendations(reviews)
	fmt.Printf("Cantidad de recomendaciones generadas para enviar: %d\n\n", len(recommendations))

	// Enviar recomendaciones en lotes y contar cuántos lotes se envían
	sendBatchedRecommendations(conn, recommendations)
	fmt.Println("Envío de recomendaciones en lotes completado.")

	// Mostrar las 5 mejores recomendaciones
	displayTopRecommendations(recommendations)
}

func displayTopRecommendations(recommendations []Recommendation) {
	// Ordenar las recomendaciones por puntuación en orden descendente
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].Score > recommendations[j].Score
	})

	fmt.Println("\n================== TOP 5 PRODUCTOS RECOMENDADOS ==================")
	for i := 0; i < 5 && i < len(recommendations); i++ {
		fmt.Printf("  Producto: %s\n", recommendations[i].ProductID)
		fmt.Printf("  Puntuación: %.2f\n", recommendations[i].Score)
		fmt.Printf("  Estrellas: %s\n", calculateStarRating(recommendations[i].Score))
		fmt.Println("---------------------------------------------------------")
	}
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

func connectToServer(address string) (net.Conn, error) {
	return net.Dial("tcp", address)
}

func receiveData(conn net.Conn) ([]byte, error) {
	fmt.Println("Recibiendo datos del servidor...")
	reader := bufio.NewReader(conn)
	return reader.ReadBytes('\n')
}

// Calcular recomendaciones concurrentemente
func calculateDistributedRecommendations(reviews []Review) []Recommendation {
	fmt.Println("Calculando recomendaciones usando ALS y similitud de coseno...")
	userRatings := make(map[string]map[string]int)
	for _, review := range reviews {
		if userRatings[review.ReviewerID] == nil {
			userRatings[review.ReviewerID] = make(map[string]int)
		}
		userRatings[review.ReviewerID][review.ProductID] = review.Stars
	}

	similarities := calculateCosineSimilarities(userRatings)
	recommendedProducts := performALS(userRatings, similarities)
	recommendations := []Recommendation{}
	for product, score := range recommendedProducts {
		recommendations = append(recommendations, Recommendation{ProductID: product, Score: score})
	}
	fmt.Println("Recomendaciones calculadas con éxito.\n")
	return recommendations
}

// Calcular similitud de coseno
func calculateCosineSimilarities(userRatings map[string]map[string]int) map[string]map[string]float64 {
	similarities := make(map[string]map[string]float64)
	for user1 := range userRatings {
		similarities[user1] = make(map[string]float64)
		for user2, ratings2 := range userRatings {
			if user1 == user2 {
				continue
			}
			numerator, sum1, sum2 := 0.0, 0.0, 0.0
			for product, rating1 := range userRatings[user1] {
				if rating2, exists := ratings2[product]; exists {
					numerator += float64(rating1 * rating2)
					sum1 += float64(rating1 * rating1)
					sum2 += float64(rating2 * rating2)
				}
			}
			if sum1 > 0 && sum2 > 0 {
				similarities[user1][user2] = numerator / (math.Sqrt(sum1) * math.Sqrt(sum2))
			}
		}
	}
	fmt.Println("Similitudes de coseno calculadas.")
	return similarities
}

// ALS
func performALS(userRatings map[string]map[string]int, similarities map[string]map[string]float64) map[string]float64 {
	recommendedProducts := make(map[string]float64)
	for user, ratings := range userRatings {
		for product := range ratings {
			for similarUser, similarity := range similarities[user] {
				if otherRating, exists := userRatings[similarUser][product]; exists {
					recommendedProducts[product] += similarity * float64(otherRating)
				}
			}
		}
	}
	return recommendedProducts
}

// Enviar recomendaciones en lotes al servidor
func sendBatchedRecommendations(conn net.Conn, recommendations []Recommendation) {
	batchCount := 0
	for i := 0; i < len(recommendations); i += batchSize {
		end := i + batchSize
		if end > len(recommendations) {
			end = len(recommendations)
		}
		batch := recommendations[i:end]
		batchJSON, err := json.Marshal(batch)
		if err != nil {
			fmt.Println("Error al convertir recomendaciones a JSON:", err)
			return
		}
		batchJSON = append(batchJSON, '\n')
		_, err = conn.Write(batchJSON)
		if err != nil {
			fmt.Println("Error al enviar lote de recomendaciones:", err)
			return
		}
		batchCount++
		fmt.Printf("Enviado lote %d con %d recomendaciones al servidor.\n", batchCount, len(batch))
		time.Sleep(1 * time.Second)
	}
	fmt.Println("Envío de todas las recomendaciones completado.")
	conn.Write([]byte("FINISHED\n"))
}
