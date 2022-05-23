package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

const (
	DB_HOST     = "localhost"
	DB_PORT     = "5432"
	DB_USER     = "postgres"
	DB_PASSWORD = ""
	DB_NAME     = "db_go_sql"
)

func main() {
	db, err := connectDB()

	if err != nil {
		panic(err)
	}

	fmt.Println("DB CONNECTED")

	employees, err := getAllEmployees(db)

	if err != nil {
		panic(err)
	}

	for _, employee := range *employees {
		employee.Print()
	}
}

func connectDB() (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

	db, err := sql.Open("postgres", dsn)

	if err != nil {
		return nil, err
	}

	// defer db.Close()

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	// connection full
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	db.SetConnMaxIdleTime(10 * time.Second)
	db.SetConnMaxLifetime(10 * time.Second)

	return db, nil
}

type Employee struct {
	ID       int    `json:"id"`
	Fullname string `json:"full_name"`
	Email    string `json:"email"`
	Age      int    `json:"age"`
	Division string `json:"division"`
}

func (e *Employee) Print() {
	fmt.Println("ID :", e.ID)
	fmt.Println("Fullname :", e.Fullname)
	fmt.Println("Email :", e.Email)
	fmt.Println("Age :", e.Age)
	fmt.Println("Division :", e.Division)
	fmt.Println()
}

func getAllEmployees(db *sql.DB) (*[]Employee, error) {
	query := `
		SELECT
			id,
			full_name,
			email,
			age,
			division
		FROM employees
	`

	smt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer smt.Close()

	var employees []Employee

	rows, err := smt.Query()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		employee := Employee{}

		err := rows.Scan(
			&employee.ID,
			&employee.Fullname,
			&employee.Email,
			&employee.Age,
			&employee.Division,
		)

		if err != nil {
			return nil, err
		}

		employees = append(employees, employee)
	}
	return &employees, nil
}
